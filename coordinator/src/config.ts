// All coordinator settings, read from the environment once at startup.
// `config` is a plain mutable object so tests can shorten timeouts in place.

function num(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`env ${name} must be a number, got "${raw}"`);
  return n;
}

function str(env: NodeJS.ProcessEnv, name: string, fallback: string): string {
  const raw = env[name];
  return raw === undefined || raw === "" ? fallback : raw;
}

function oneOf<T extends string>(env: NodeJS.ProcessEnv, name: string, allowed: readonly T[]): T {
  const v = str(env, name, allowed[0]);
  if (!(allowed as readonly string[]).includes(v)) throw new Error(`env ${name} must be one of ${allowed.join(", ")}, got "${v}"`);
  return v as T;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  return {
    port: num(env, "PORT", 3000),
    databaseUrl: str(env, "DATABASE_URL", "postgres://wildebeest:wildebeest@localhost:5432/wildebeest"),
    redisUrl: str(env, "REDIS_URL", "redis://localhost:6379"),

    s3Endpoint: str(env, "S3_ENDPOINT", "http://localhost:9000"),
    s3PublicEndpoint: str(env, "S3_PUBLIC_ENDPOINT", str(env, "S3_ENDPOINT", "http://localhost:9000")),
    s3AccessKey: str(env, "S3_ACCESS_KEY", "minioadmin"),
    s3SecretKey: str(env, "S3_SECRET_KEY", "minioadmin"),
    s3Bucket: str(env, "S3_BUCKET", "wildebeest"),
    sampleDir: str(env, "SAMPLE_DIR", "/data/sample"),

    detectorModelVersion: str(env, "DETECTOR_MODEL_VERSION", "speciesnet-md_v5a"),
    classifierModelVersion: str(env, "CLASSIFIER_MODEL_VERSION", "speciesnet-v4.0.1a"),

    leaseMs: num(env, "LEASE_MS", 15000),
    heartbeatMs: num(env, "HEARTBEAT_MS", 2000),
    workerTimeoutMs: num(env, "WORKER_TIMEOUT_MS", 6000),
    maxAttempts: num(env, "MAX_ATTEMPTS", 3),
    // Claim batch: workers size it online (≈ round trip ÷ service time, RabbitMQ's prefetch rule)
    // between this floor and MAX_CLAIM_BATCH. Equal values pin it.
    claimBatchSize: num(env, "CLAIM_BATCH_SIZE", 1),
    maxClaimBatch: num(env, "MAX_CLAIM_BATCH", 16),
    animalConfThreshold: num(env, "ANIMAL_CONF_THRESHOLD", 0.2),
    classifyQueueHighWater: num(env, "CLASSIFY_QUEUE_HIGH_WATER", 500),
    classifyQueueLowWater: num(env, "CLASSIFY_QUEUE_LOW_WATER", 200),
    // queue:detect depth. 0 (default) = scale with live detect workers × their claim batch, never
    // below DETECT_QUEUE_MIN; a positive value pins it (the P1 behaviour, and what tests use).
    detectQueueTarget: num(env, "DETECT_QUEUE_TARGET", 0),
    detectQueueMin: num(env, "DETECT_QUEUE_MIN", 8),
    defaultCountry: str(env, "DEFAULT_COUNTRY", "TZA"),
    humanReviewSecondsPerImage: num(env, "HUMAN_REVIEW_SECONDS_PER_IMAGE", 3),

    dispatchIntervalMs: num(env, "DISPATCH_INTERVAL_MS", 200),
    reapIntervalMs: num(env, "REAP_INTERVAL_MS", 1000),
    dockerSocket: str(env, "DOCKER_SOCKET", "/var/run/docker.sock"),
    // Docker `die`/`oom` events are the fast failure detector. "auto" = on when the coordinator can
    // see the Docker socket and find its own Compose project; "off" = heartbeats only.
    dockerEvents: str(env, "DOCKER_EVENTS", "auto"),
    // Override for the Compose project whose containers we watch (normally read from our own
    // container's `com.docker.compose.project` label).
    composeProject: str(env, "COMPOSE_PROJECT", ""),

    // Retry hygiene: full-jitter backoff before a failed task is re-dispatched,
    // delay = random(0, min(RETRY_MAX_MS, RETRY_BASE_MS * 2^(taskErrors-1))).
    retryBaseMs: num(env, "RETRY_BASE_MS", 500),
    retryMaxMs: num(env, "RETRY_MAX_MS", 30000),
    maxPauseMs: num(env, "MAX_PAUSE_MS", 120000),

    // Telemetry (GET /system and the websocket `system` message).
    // hybrid: Redis ready queues + claim-confirm. postgres: no Redis queue, workers long-poll
    // POST /tasks/claim (one UPDATE … FOR UPDATE SKIP LOCKED). Same leases and fencing either way.
    claimMode: oneOf(env, "CLAIM_MODE", ["hybrid", "postgres"] as const),
    // push: task IDs are pushed right after the commit that made them PENDING; the 200 ms tick
    // is only a repair sweep. tick: the P1 dispatcher (the tick does all dispatching).
    dispatchMode: oneOf(env, "DISPATCH_MODE", ["push", "tick"] as const),
    /** Longest a POST /tasks/claim long-poll may wait. */
    claimWaitMaxMs: num(env, "CLAIM_WAIT_MAX_MS", 5000),
    modelBackend: str(env, "MODEL_BACKEND", "speciesnet"),
    systemIntervalMs: num(env, "SYSTEM_INTERVAL_MS", 500),
    invariantIntervalMs: num(env, "INVARIANT_INTERVAL_MS", 5000),
    maxSyntheticTasks: num(env, "MAX_SYNTHETIC_TASKS", 1_000_000),

    // Straggler speculation (docs/decisions/p3-speculation.md). When a stage's ready queue is
    // empty and a worker is idle, a task running longer than max(SPECULATE_MIN_MS,
    // SPECULATE_MULTIPLIER × the stage's p50 service time) gets one speculative copy on the fastest
    // idle worker; the first result wins. A worker whose p50 exceeds
    // SPECULATE_PROBATION_MULTIPLIER × the stage p50 is on probation: it gets no copies.
    speculation: oneOf(env, "SPECULATION", ["on", "off"] as const),
    speculateMultiplier: num(env, "SPECULATE_MULTIPLIER", 3),
    speculateMinMs: num(env, "SPECULATE_MIN_MS", 1000),
    speculateMinSamples: num(env, "SPECULATE_MIN_SAMPLES", 5),
    speculateProbationMultiplier: num(env, "SPECULATE_PROBATION_MULTIPLIER", 3),
    speculateIntervalMs: num(env, "SPECULATE_INTERVAL_MS", 250),
    /** An offer the target worker hasn't claimed by then is dropped (and may go to another worker). */
    speculateOfferTtlMs: num(env, "SPECULATE_OFFER_TTL_MS", 3000),

    // High availability (docs/decisions/h-ha.md). Every replica serves the API; one leader, elected
    // through a lease row in Postgres, runs the singleton loops. COORDINATOR_ID names this replica
    // (default: the hostname, i.e. the container ID); the lease lasts LEADER_TTL_MS and the leader
    // renews it every LEADER_RENEW_MS.
    coordinatorId: str(env, "COORDINATOR_ID", ""),
    leaderTtlMs: num(env, "LEADER_TTL_MS", 5000),
    leaderRenewMs: num(env, "LEADER_RENEW_MS", 1000),
  };
}

export type Config = ReturnType<typeof loadConfig>;

export const config: Config = loadConfig();
