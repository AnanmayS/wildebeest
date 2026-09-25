// Runs before each test file's imports, so src/config.ts picks these up.
// Defaults match the host ports docker-compose publishes; override with TEST_* env vars.
process.env.DATABASE_URL = process.env.TEST_DATABASE_URL ?? "postgres://wildebeest:wildebeest@localhost:15432/wildebeest_test";
process.env.REDIS_URL = process.env.TEST_REDIS_URL ?? "redis://localhost:16379/15";
process.env.S3_ENDPOINT = process.env.TEST_S3_ENDPOINT ?? "http://localhost:9000";
process.env.S3_PUBLIC_ENDPOINT = process.env.S3_ENDPOINT;
process.env.S3_BUCKET = "wildebeest-test";
process.env.DETECTOR_MODEL_VERSION = "test-detector-v1";
process.env.CLASSIFIER_MODEL_VERSION = "test-classifier-v1";
process.env.LEASE_MS = "3000";
process.env.HEARTBEAT_MS = "500";
process.env.WORKER_TIMEOUT_MS = "2000";
process.env.MAX_ATTEMPTS = "3";
process.env.DETECT_QUEUE_TARGET = "5";
process.env.CLASSIFY_QUEUE_HIGH_WATER = "5";
process.env.CLASSIFY_QUEUE_LOW_WATER = "2";
