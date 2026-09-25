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
    claimBatchSize: num(env, "CLAIM_BATCH_SIZE", 1),
    animalConfThreshold: num(env, "ANIMAL_CONF_THRESHOLD", 0.2),
    classifyQueueHighWater: num(env, "CLASSIFY_QUEUE_HIGH_WATER", 500),
    classifyQueueLowWater: num(env, "CLASSIFY_QUEUE_LOW_WATER", 200),
    detectQueueTarget: num(env, "DETECT_QUEUE_TARGET", 50),
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
    claimMode: str(env, "CLAIM_MODE", "hybrid"),
    modelBackend: str(env, "MODEL_BACKEND", "speciesnet"),
    systemIntervalMs: num(env, "SYSTEM_INTERVAL_MS", 500),
    invariantIntervalMs: num(env, "INVARIANT_INTERVAL_MS", 5000),
    maxSyntheticTasks: num(env, "MAX_SYNTHETIC_TASKS", 1_000_000),
  };
}

export type Config = ReturnType<typeof loadConfig>;

export const config: Config = loadConfig();
