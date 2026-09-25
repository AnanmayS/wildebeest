-- P3: straggler speculation. A task may now have two valid attempts at once: its lease (the
-- tasks row, unchanged) and at most one speculative copy (a task_attempts row). The first result
-- to commit wins; the other attempt is told it lost (ALREADY_DONE) and to cancel.
-- docs/decisions/p3-speculation.md.
--
-- Model
--   tasks.worker_id / lease_epoch / lease_expires_at / started_at keep describing the task's lease
--   exactly as before, so every single-attempt path (claim, complete, fail, release, heartbeat,
--   reaper) is untouched for tasks that are never speculated.
--   tasks.spec_epoch is the epoch reserved for the task's speculative copy (null = never
--   speculated). Epochs stay unique per task: a claim now takes greatest(lease_epoch, spec_epoch) + 1.
--   task_attempts holds the copy. It "shadows" the lease it was started next to (shadow_epoch) and
--   is valid only while that lease is still the task's lease:
--
--       valid(copy) = copy.state = 'running' and task.state = 'LEASED' and task.lease_epoch = copy.shadow_epoch
--
--   So anything that ends the lease (fail, release, requeue, a later claim) invalidates the copy
--   without having to touch task_attempts. Two exceptions change the lease on purpose:
--     - the copy wins: the task becomes SUCCEEDED with the copy's worker/epoch (copy 'won');
--     - the original loses its lease while the copy is healthy: the copy is promoted to be the
--       lease (tasks.worker_id/lease_epoch := the copy's; copy 'promoted'), nothing is requeued.
--
-- States: offered (reserved for an idle worker, not yet claimed) → running → won | lost | promoted
-- | dropped. 'lost' = the other attempt's result committed first (its late report gets
-- ALREADY_DONE); 'dropped' = offer never taken, copy failed/released, its lease expired, its
-- worker died, or its task moved on under it (reports get STALE_LEASE).

alter table tasks add column spec_epoch int null;

create table task_attempts (
  task_id          uuid not null references tasks(id) on delete cascade,
  epoch            int not null,
  worker_id        text not null,
  shadow_epoch     int not null,
  state            text not null check (state in ('offered', 'running', 'won', 'lost', 'promoted', 'dropped')),
  offered_at       timestamptz not null default now(),
  started_at       timestamptz null,
  lease_expires_at timestamptz null,
  finished_at      timestamptz null,
  end_reason       text null,
  -- Why it was launched: { ageMs, thresholdMs, stageP50Ms, originalWorker, originalEpoch }.
  detail           jsonb null,
  primary key (task_id, epoch)
);

-- Open attempts per worker: offers to pick up, copies to renew / cancel / call "busy".
create index task_attempts_open_idx on task_attempts (worker_id) where state in ('offered', 'running');
-- Reaper sweep over open attempts.
create index task_attempts_open_state_idx on task_attempts (state) where state in ('offered', 'running');

-- ---------------------------------------------------------------------------------------------
-- Late reports: lost the race, or fenced off?
-- ---------------------------------------------------------------------------------------------
--
-- A report (complete / fail / release) that matched no valid attempt is 'already_done' when the
-- attempt it carries was still valid at the moment the task's other attempt won: it is the copy
-- that lost ('lost'), or the original a winning copy shadowed. Everything else is 'stale'
-- (STALE_LEASE), exactly as before: an old epoch, a lease lost before the win (e.g. an original
-- paused past its lease, whose copy was then promoted), a dropped copy.
create or replace function wb_late_outcome(p_task uuid, p_epoch int) returns text
language sql stable as $$
  select case
    when exists (select 1 from tasks t
                  where t.id = p_task and t.state = 'SUCCEEDED' and t.spec_epoch is not null
                    and exists (select 1 from task_attempts a
                                 where a.task_id = t.id
                                   and ((a.state = 'lost' and a.epoch = p_epoch)
                                        or (a.state = 'won' and a.shadow_epoch = p_epoch))))
    then 'already_done' else 'stale' end
$$;

-- ---------------------------------------------------------------------------------------------
-- wb_complete, now multi-attempt (replaces 006's)
-- ---------------------------------------------------------------------------------------------
--
-- Differences from 006, all confined to speculated tasks (spec_epoch is not null):
--   - An item whose epoch is not the lease's may be the task's running copy: if the copy is valid
--     (see above) its result wins: task SUCCEEDED with the copy's worker, epoch and start time,
--     copy 'won', event `speculation_won`.
--   - When the lease's own result wins on a task with a launched copy, the copy becomes 'lost' and
--     the event is `speculation_wasted` (or `speculation_won` if the lease *was* the promoted copy).
--   - A fenced item gets status 'already_done' (no event: the win already has one) when it lost a
--     race, else 'stale' with its `stale_rejected` event as before.
-- The speculation event rides in the item's event_* columns (ok rows carried none before).
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
  sa record;
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
  v_ok boolean;
  v_spec_type text;
  v_spec_detail jsonb;
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
    v_spec_type := null; v_spec_detail := null;

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

    -- The lease's own result (the only path for a task that was never speculated).
    update tasks x set state = 'SUCCEEDED', finished_at = now(), lease_expires_at = null, queued = false,
                       timings = case when jsonb_typeof(it.timings) = 'object' then it.timings end
     where x.id = it.id and x.state = 'LEASED' and x.lease_epoch = it.epoch
    returning x.image_id, x.worker_id, x.pending_at, x.not_before, x.eligible_at, x.pushed_at, x.started_at,
              x.spec_epoch, x.lease_epoch
      into t;
    v_ok := found;

    if v_ok and t.spec_epoch is not null then
      if t.spec_epoch = it.epoch then
        -- The lease was a promoted copy: speculation saved this task.
        v_spec_type := 'speculation_won';
        select jsonb_build_object('stage', it.task_stage, 'winner', t.worker_id, 'epoch', it.epoch, 'promoted', true,
                                  'originalWorker', a.detail->>'originalWorker', 'originalEpoch', a.shadow_epoch)
          into v_spec_detail
          from task_attempts a where a.task_id = it.id and a.epoch = it.epoch;
      else
        -- The original finished first. A copy that was launched (running, or already dropped) was wasted.
        update task_attempts a set state = 'lost', finished_at = now(), end_reason = 'original finished first'
         where a.task_id = it.id and a.state = 'running';
        select jsonb_build_object('stage', it.task_stage, 'winner', t.worker_id, 'epoch', it.epoch,
                                  'speculativeWorker', a.worker_id, 'speculativeEpoch', a.epoch)
          into v_spec_detail
          from task_attempts a where a.task_id = it.id and a.started_at is not null
         order by a.epoch desc limit 1;
        if v_spec_detail is not null then v_spec_type := 'speculation_wasted'; end if;
      end if;
    end if;

    if not v_ok then
      -- Maybe the task's speculative copy: valid while the lease it shadows is still the lease.
      update task_attempts a set state = 'won', finished_at = now(), end_reason = 'finished first'
        from tasks x
       where a.task_id = it.id and a.epoch = it.epoch and a.state = 'running'
         and x.id = a.task_id and x.state = 'LEASED' and x.lease_epoch = a.shadow_epoch
      returning a.worker_id, a.started_at, a.shadow_epoch, x.worker_id as original_worker,
                (extract(epoch from (now() - x.started_at)) * 1000)::int as original_age_ms
        into sa;
      if found then
        update tasks x set state = 'SUCCEEDED', finished_at = now(), lease_expires_at = null, queued = false,
                           timings = case when jsonb_typeof(it.timings) = 'object' then it.timings end,
                           worker_id = sa.worker_id, lease_epoch = it.epoch, started_at = sa.started_at
         where x.id = it.id
        returning x.image_id, x.worker_id, x.pending_at, x.not_before, x.eligible_at, x.pushed_at, x.started_at,
                  x.spec_epoch, x.lease_epoch
          into t;
        v_ok := true;
        v_spec_type := 'speculation_won';
        v_spec_detail := jsonb_build_object('stage', it.task_stage, 'winner', sa.worker_id, 'epoch', it.epoch,
                                            'originalWorker', sa.original_worker, 'originalEpoch', sa.shadow_epoch,
                                            'originalAgeMs', sa.original_age_ms, 'promoted', false);
      end if;
    end if;

    if not v_ok then
      -- Fenced off. Lost a race (the other attempt's result is in) → already_done, no event.
      -- Otherwise an old epoch (reassigned) or the task already finished → stale, as before.
      if wb_late_outcome(it.id, it.epoch) = 'already_done' then
        status := 'already_done';
        return next;
        continue;
      end if;
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
    if v_spec_type is not null then
      insert into task_events (task_id, worker_id, type, detail)
      values (it.id, t.worker_id, v_spec_type, v_spec_detail)
      returning task_events.id, task_events.at into event_id, event_at;
      event_type := v_spec_type; event_detail := v_spec_detail;
    end if;
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
