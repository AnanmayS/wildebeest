import http from "node:http";
import { createApp } from "./api.js";
import { stopChaos } from "./chaos.js";
import { config } from "./config.js";
import { closePool, migrate } from "./db.js";
import { hub } from "./events.js";
import { jobSummary } from "./jobs.js";
import { reconcileOnStartup, startLoops } from "./loops.js";
import { closeRedis } from "./redis.js";
import { ensureBucket } from "./storage.js";
import { listWorkers } from "./workers.js";

async function waitFor<T>(what: string, fn: () => Promise<T>, attempts = 60): Promise<T> {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i >= attempts) throw err;
      console.log(`[startup] waiting for ${what}: ${(err as Error).message}`);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

async function main() {
  const applied = await waitFor("postgres", () => migrate());
  console.log(`[startup] migrations applied: ${applied.length ? applied.join(", ") : "none pending"}`);
  await ensureBucket();
  await reconcileOnStartup();

  const app = createApp();
  const server = http.createServer(app);
  hub.setBuilders({ jobSummary, workerList: listWorkers });
  hub.attach(server);
  const stopLoops = startLoops();

  server.listen(config.port, () => {
    console.log(
      `[startup] coordinator on :${config.port} (lease ${config.leaseMs} ms, heartbeat ${config.heartbeatMs} ms, ` +
        `worker timeout ${config.workerTimeoutMs} ms, models ${config.detectorModelVersion} / ${config.classifierModelVersion})`,
    );
  });

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[shutdown] ${signal}`);
    stopLoops();
    stopChaos();
    hub.close();
    server.close();
    await Promise.allSettled([closePool(), closeRedis()]);
    process.exit(0);
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  console.error("[startup] fatal:", err);
  process.exit(1);
});
