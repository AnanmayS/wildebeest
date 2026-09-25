import { Redis } from "ioredis";
import { config } from "./config.js";

// Redis is used only as plain storage: two ready-queue lists, one processing list per worker,
// a liveness key per worker, and an informational throttle flag. See docs/CONTRACTS.md.
export const keys = {
  queue: (stage: string) => `queue:${stage}`,
  processing: (workerId: string) => `processing:${workerId}`,
  alive: (workerId: string) => `worker:${workerId}:alive`,
  throttled: "forgegrid:throttled",
};

let client: Redis | null = null;

export function getRedis(): Redis {
  if (!client) {
    client = new Redis(config.redisUrl, { maxRetriesPerRequest: 3 });
    client.on("error", (err) => console.error("[redis]", err.message));
  }
  return client;
}

export async function closeRedis() {
  if (client) {
    const c = client;
    client = null;
    await c.quit().catch(() => c.disconnect());
  }
}
