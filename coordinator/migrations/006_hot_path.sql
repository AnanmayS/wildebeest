-- P2: hot-path throughput. Queue-table hygiene, indexes for the Postgres claim mode, and the
-- single-statement completion (wb_complete) with its helpers. docs/decisions/p2-hotpath.md.

-- ---------------------------------------------------------------------------------------------
-- Queue hygiene
-- ---------------------------------------------------------------------------------------------

-- tasks is a queue table: every task is updated 3-5 times (push, claim, lease renewals, complete).
-- Vacuum after 1% of the rows are dead instead of the default 20%, and leave 30% of every page free
-- so an update can stay on its page (a HOT update: no new index entries, cheap to prune).
-- fillfactor only applies to pages written from now on; existing pages fill up as they are rewritten.
alter table tasks set (
  fillfactor = 70,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.02
);

-- Lease renewal (the heartbeat's only write to tasks) touches lease_expires_at alone. With this
-- index in place no renewal could ever be HOT, because an indexed column changes. The reaper finds
-- expired leases through tasks_leased_worker_idx instead: it covers every LEASED row, and there are
-- only ever about workers × claim batch of those.
drop index if exists tasks_leased_expiry_idx;

-- task_events is append-only (never updated or deleted), so it produces no dead tuples; what it
-- needs is frequent insert-driven vacuums to keep the visibility map current for index-only scans,
-- and fresh statistics for the invariant checker's id-range scans.
alter table task_events set (
  autovacuum_vacuum_insert_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02
);

-- One row per worker, rewritten on every heartbeat and every completion.
alter table workers set (fillfactor = 50);

-- CLAIM_MODE=postgres claims retries (tasks that ran before) ahead of new work, oldest first.
-- New work uses tasks_dispatch_idx (stage, enqueued_at) where PENDING and not queued.
create index if not exists tasks_retry_idx on tasks (stage, pending_at)
  where state = 'PENDING' and queued = false and started_at is not null;

-- images(job_id) where final_category is null already exists (images_job_unfinished_idx, 001):
-- wb_finish_job's bounded count uses it.

-- ---------------------------------------------------------------------------------------------
-- Categorisation and finalisation (the SQL twins of results.ts; a test keeps the two in step)
-- ---------------------------------------------------------------------------------------------

-- CONTRACTS.md "Final categorisation after stage 1": animal > human > vehicle > empty.
create or replace function wb_categorize(p_detections jsonb, p_threshold float8) returns text
language sql immutable as $$
  select case
    when exists (select 1 from jsonb_array_elements(p_detections) d
                  where d->>'label' = 'animal' and (d->>'conf')::float8 >= p_threshold) then 'animal'
    when exists (select 1 from jsonb_array_elements(p_detections) d
                  where d->>'label' = 'human' and (d->>'conf')::float8 >= p_threshold) then 'human'
    when exists (select 1 from jsonb_array_elements(p_detections) d
                  where d->>'label' = 'vehicle' and (d->>'conf')::float8 >= p_threshold) then 'vehicle'
    else 'empty'
  end
$$;

-- Sets an image's final category once; returns its job ID if this call finalised it. A stage 2
-- "blank" overrules the detector (DECISIONS #34): the photo is empty, species fields cleared.
create or replace function wb_finalize_image(
  p_image uuid, p_category text, p_label text default null, p_common text default null, p_conf real default null
) returns uuid
language plpgsql as $$
declare
  v_job uuid;
begin
  if p_category = 'animal' and lower(p_common) = 'blank' then
    p_category := 'empty';
    p_label := null; p_common := null; p_conf := null;
  end if;
  update images set final_category = p_category, species_label = p_label, species_common_name = p_common,
                    species_conf = p_conf, finalized_at = now()
   where id = p_image and final_category is null
  returning job_id into v_job;
  return v_job;
end
$$;

-- Marks a job done if none of its images is left unfinalised, without locking the job row on
-- every call. Far from the end of a job (more unfinalised images than could possibly be finishing
-- at this moment, p_lock_threshold) there is nothing to do, and the bounded count below costs at
-- most p_lock_threshold + 1 index entries. Near the end, the job row is locked so two "last"
-- images committing at once are serialised: the second waits, then re-checks with a fresh
-- snapshot (a new statement in a volatile function) that sees the first one's commit. If the
-- threshold was too low and both transactions skipped the lock, the reaper's 1 s sweep
-- (finishCompletedJobs) marks the job done instead.
create or replace function wb_finish_job(p_job uuid, p_lock_threshold int)
returns table (name text, total int)
language plpgsql as $$
begin
  if (select count(*) from (select 1 from images i
                             where i.job_id = p_job and i.final_category is null
                             limit greatest(p_lock_threshold, 0) + 1) near) > greatest(p_lock_threshold, 0) then
    return;
  end if;
  perform 1 from jobs j where j.id = p_job for update;
  return query
    update jobs j set status = 'done', finished_at = now()
     where j.id = p_job and j.status = 'running'
       and not exists (select 1 from images i where i.job_id = p_job and i.final_category is null)
    returning j.name, j.total_images;
end
$$;

-- ---------------------------------------------------------------------------------------------
-- wb_complete: one statement per complete request (single or batch)
-- ---------------------------------------------------------------------------------------------
--
-- p_items: [{ "taskId", "leaseEpoch", "detections": [...] | null, "classification": {...} | null,
--             "timings": {...} | null }], already validated and normalised by the coordinator.
--
-- Per item, exactly what completeTask used to do over ~12 round trips:
--   fenced UPDATE LEASED → SUCCEEDED (lease_epoch must match), idempotent result insert
--   (first result per (sha256, model_version) wins; the image is categorised from the stored row),
--   finalise the image or create its classify task, count the completion, log `succeeded`.
-- A stale or unknown item changes nothing and never fails the batch: it gets a `stale` row (with
-- its `stale_rejected` event) or a `not_found` row. A detect task without detections is `invalid`
-- and keeps its lease. After all items, each touched job is checked once for completion
-- (wb_finish_job), in job-id order.
--
-- Locking: the batch's task rows are locked up front in id order, results are inserted in sha256
-- order and jobs are checked in id order, so two concurrent batches always take shared locks in
-- the same order. Heartbeats renew leases with SKIP LOCKED and never wait on a completing batch.
--
-- Output: one row per item (status ok | stale | not_found | invalid) plus one `job_done` row per
-- job this call finished. `event_*` carries the dashboard-visible event of that row, if any.
create or replace function wb_complete(
  p_worker text,
  p_items jsonb,
  p_detector_version text,
  p_classifier_version text,
  p_threshold float8,
  p_finish_lock_threshold int
) returns table (
  task_id uuid, status text, stage text, job_id uuid, finalised boolean, classify_task_id uuid,
  pending_at timestamptz, not_before timestamptz, eligible_at timestamptz, pushed_at timestamptz,
  started_at timestamptz,
  event_id bigint, event_at timestamptz, event_type text, event_detail jsonb
)
language plpgsql as $$
#variable_conflict use_column
declare
  it record;
  t record;
  cur record;
  stored jsonb;
  cls record;
  category text;
  v_job uuid;
  v_final uuid;
  v_classify uuid;
  v_jobs uuid[] := '{}';
  v_holders text[] := '{}';
  v_detail jsonb;
begin
  perform 1 from tasks x
   where x.id in (select (e->>'taskId')::uuid from jsonb_array_elements(p_items) e)
   order by x.id
   for update;

  for it in
    select (e->>'taskId')::uuid as id, (e->>'leaseEpoch')::int as epoch,
           e->'detections' as detections, e->'classification' as cls, e->'timings' as timings,
           x.stage as task_stage, i.sha256, i.job_id as img_job
      from jsonb_array_elements(p_items) e
      left join tasks x on x.id = (e->>'taskId')::uuid
      left join images i on i.id = x.image_id
     order by i.sha256, e->>'taskId'
  loop
    task_id := it.id; stage := it.task_stage; job_id := it.img_job; finalised := false;
    classify_task_id := null; pending_at := null; not_before := null; eligible_at := null;
    pushed_at := null; started_at := null;
    event_id := null; event_at := null; event_type := null; event_detail := null;

    if it.task_stage is null then
      status := 'not_found';
      return next;
      continue;
    end if;
    if it.task_stage = 'detect' and jsonb_typeof(it.detections) is distinct from 'array' then
      status := 'invalid';
      return next;
      continue;
    end if;

    update tasks x set state = 'SUCCEEDED', finished_at = now(), lease_expires_at = null, queued = false,
                       timings = case when jsonb_typeof(it.timings) = 'object' then it.timings end
     where x.id = it.id and x.state = 'LEASED' and x.lease_epoch = it.epoch
    returning x.image_id, x.worker_id, x.pending_at, x.not_before, x.eligible_at, x.pushed_at, x.started_at
      into t;

    if not found then
      -- Fenced off: an old epoch (the task was reassigned), or the task already finished.
      select x.state, x.lease_epoch, x.worker_id into cur from tasks x where x.id = it.id;
      v_detail := jsonb_build_object('action', 'complete', 'leaseEpoch', it.epoch, 'currentEpoch', cur.lease_epoch,
                                     'state', cur.state, 'holder', cur.worker_id);
      insert into task_events (task_id, worker_id, type, detail)
      values (it.id, p_worker, 'stale_rejected', v_detail)
      returning task_events.id, task_events.at into event_id, event_at;
      status := 'stale'; event_type := 'stale_rejected'; event_detail := v_detail;
      return next;
      continue;
    end if;

    pending_at := t.pending_at; not_before := t.not_before; eligible_at := t.eligible_at;
    pushed_at := t.pushed_at; started_at := t.started_at;
    v_final := null;

    if it.task_stage = 'detect' then
      insert into detection_results (sha256, model_version, detections)
      values (it.sha256, p_detector_version, it.detections)
      on conflict (sha256, model_version) do nothing;
      select d.detections into stored from detection_results d
       where d.sha256 = it.sha256 and d.model_version = p_detector_version;
      category := wb_categorize(stored, p_threshold);

      if category = 'animal' then
        -- Another job may already have classified this exact photo.
        select c.label, c.common_name, c.confidence into cls from classification_results c
         where c.sha256 = it.sha256 and c.model_version = p_classifier_version;
        if found then
          v_final := wb_finalize_image(t.image_id, 'animal', cls.label, cls.common_name, cls.confidence);
        else
          insert into tasks (id, image_id, stage, state) values (gen_random_uuid(), t.image_id, 'classify', 'PENDING')
          on conflict on constraint tasks_image_id_stage_key do nothing
          returning tasks.id into v_classify;
          if v_classify is not null then
            classify_task_id := v_classify;
            insert into task_events (task_id, type, detail)
            values (v_classify, 'enqueued', jsonb_build_object('stage', 'classify'));
          end if;
        end if;
      else
        v_final := wb_finalize_image(t.image_id, category);
      end if;
    else
      insert into classification_results (sha256, model_version, label, common_name, confidence, crop_key, raw)
      values (it.sha256, p_classifier_version, it.cls->>'label', it.cls->>'commonName',
              (it.cls->>'confidence')::real, it.cls->>'cropKey', coalesce(it.cls->'raw', 'null'::jsonb))
      on conflict (sha256, model_version) do nothing;
      select c.label, c.common_name, c.confidence into cls from classification_results c
       where c.sha256 = it.sha256 and c.model_version = p_classifier_version;
      v_final := wb_finalize_image(t.image_id, 'animal', cls.label, cls.common_name, cls.confidence);
    end if;

    insert into task_events (task_id, worker_id, type, detail)
    values (it.id, t.worker_id, 'succeeded', jsonb_build_object('stage', it.task_stage, 'leaseEpoch', it.epoch));
    v_holders := v_holders || t.worker_id;
    if v_final is not null then
      finalised := true;
      v_jobs := v_jobs || v_final;
    end if;
    status := 'ok';
    return next;
  end loop;

  -- Credit the lease holders (normally all the sending worker): one row update per worker.
  update workers w set tasks_completed = w.tasks_completed + c.n
    from (select h, count(*)::int as n from unnest(v_holders) h group by h) c
   where w.id = c.h;

  -- Job completion, once per touched job, in a fixed order.
  for v_job in select distinct j from unnest(v_jobs) j order by j loop
    for cur in select * from wb_finish_job(v_job, p_finish_lock_threshold) loop
      v_detail := jsonb_build_object('jobId', v_job, 'name', cur.name, 'total', cur.total);
      insert into task_events (type, detail) values ('job_done', v_detail)
      returning task_events.id, task_events.at into event_id, event_at;
      task_id := null; status := 'job_done'; stage := null; job_id := v_job; finalised := false;
      classify_task_id := null; pending_at := null; not_before := null; eligible_at := null;
      pushed_at := null; started_at := null; event_type := 'job_done'; event_detail := v_detail;
      return next;
    end loop;
  end loop;
end
$$;
