-- Wildebeest schema (PRD section 9 plus the coordinator's bookkeeping columns).

create table jobs (
  id            uuid primary key,
  name          text not null,
  status        text not null default 'running',          -- 'running' | 'done'
  created_at    timestamptz not null default now(),
  finished_at   timestamptz null,
  total_images  int not null default 0,
  country_code  text null,
  sample_size   int null                                   -- set for POST /jobs/sample jobs
);

create table images (
  id                  uuid primary key,
  job_id              uuid not null references jobs(id) on delete cascade,
  sha256              text not null,
  object_key          text not null,
  original_name       text null,
  final_category      text null,                            -- 'empty'|'animal'|'human'|'vehicle'|'failed'
  species_label       text null,
  species_common_name text null,
  species_conf        real null,
  cache_hit           boolean not null default false,
  created_at          timestamptz not null default now(),
  finalized_at        timestamptz null,
  constraint images_final_category_chk
    check (final_category is null or final_category in ('empty', 'animal', 'human', 'vehicle', 'failed'))
);
create index images_job_idx on images (job_id);
create index images_job_unfinished_idx on images (job_id) where final_category is null;
create index images_job_finalized_idx on images (job_id, finalized_at);
create index images_sha_idx on images (sha256);

create table tasks (
  id               uuid primary key,
  image_id         uuid not null references images(id) on delete cascade,
  stage            text not null check (stage in ('detect', 'classify')),
  state            text not null default 'PENDING' check (state in ('PENDING', 'LEASED', 'SUCCEEDED', 'FAILED')),
  attempts         int not null default 0,
  lease_epoch      int not null default 0,
  worker_id        text null,
  lease_expires_at timestamptz null,
  enqueued_at      timestamptz not null default now(),
  started_at       timestamptz null,
  finished_at      timestamptz null,
  error            text null,
  -- true while the task ID sits in a Redis ready queue or a worker's processing list
  queued           boolean not null default false,
  unique (image_id, stage)
);
-- dispatcher: PENDING tasks not yet pushed to Redis, oldest first
create index tasks_dispatch_idx on tasks (stage, enqueued_at) where state = 'PENDING' and queued = false;
create index tasks_state_stage_idx on tasks (state, stage);
-- reaper + heartbeat: live leases by worker / by expiry
create index tasks_leased_worker_idx on tasks (worker_id) where state = 'LEASED';
create index tasks_leased_expiry_idx on tasks (lease_expires_at) where state = 'LEASED';

create table detection_results (
  sha256        text not null,
  model_version text not null,
  detections    jsonb not null,
  created_at    timestamptz not null default now(),
  primary key (sha256, model_version)
);

create table classification_results (
  sha256        text not null,
  model_version text not null,
  label         text null,
  common_name   text null,
  confidence    real null,
  crop_key      text null,
  raw           jsonb null,
  created_at    timestamptz not null default now(),
  primary key (sha256, model_version)
);

create table workers (
  id                text primary key,
  stage             text not null,
  hostname          text null,
  container_id      text null,
  status            text not null default 'ALIVE' check (status in ('ALIVE', 'DEAD', 'STOPPED')),
  registered_at     timestamptz not null default now(),
  last_heartbeat_at timestamptz not null default now(),
  tasks_completed   int not null default 0,
  dead_at           timestamptz null,
  killed_at         timestamptz null,
  reassigned_count  int not null default 0,
  metrics           jsonb not null default '{}'::jsonb
);
create index workers_status_idx on workers (status, last_heartbeat_at);

create table task_events (
  id        bigserial primary key,
  task_id   uuid null,          -- null for job/worker/throttle-level events
  worker_id text null,
  type      text not null,
  at        timestamptz not null default now(),
  detail    jsonb null
);
create index task_events_task_idx on task_events (task_id, type);
create index task_events_worker_idx on task_events (worker_id, type);
create index task_events_type_idx on task_events (type, id);
