import http from "node:http";
import { createApp } from "./api.js";
import { stopChaos } from "./chaos.js";
import { stopDeathWatch } from "./deathwatch.js";
import { resumeAll } from "./docker.js";
import { config } from "./config.js";
import { closePool, migrate, nodeId } from "./db.js";
import { hub } from "./events.js";
import { startHa } from "./ha.js";
import { jobSummary } from "./jobs.js";
import { closeRedis } from "./redis.js";
import { ensureBucket } from "./storage.js";
import { systemSnapshot } from "./system.js";
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

  const app = createApp();
  const server = http.createServer(app);
  hub.setBuilders({ jobSummary, workerList: listWorkers, system: systemSnapshot });
  hub.attach(server);
  // Join the cluster: the bus, the replica loops, and a first election round. Whoever wins runs
  // the startup reconciliation (queue rebuild, grace) and the singleton loops; a follower's start
  // touches no shared state (docs/decisions/h-ha.md).
  const ha = await startHa();

  server.listen(config.port, () => {
    console.log(
      `[startup] coordinator ${nodeId()} (${ha.elector.role}) on :${config.port} (lease ${config.leaseMs} ms, ` +
        `heartbeat ${config.heartbeatMs} ms, worker timeout ${config.workerTimeoutMs} ms, ` +
        `models ${config.detectorModelVersion} / ${config.classifierModelVersion})`,
    );
  });

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[shutdown] ${signal}`);
    await ha.stop(); // a leader resigns, so a follower takes over within one election round
    stopChaos();
    stopDeathWatch();
    await resumeAll();
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
