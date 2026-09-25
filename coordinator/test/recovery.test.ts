// P1 recovery fast path: Docker `die`/`oom` events (mocked event stream), `docker ps`
// reconciliation, attribution of our own kills/pauses, and the reaper's self-awareness.
import { PassThrough } from "node:stream";
import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { query } from "../src/db.js";
import { DeathWatch, dockerSince, PROJECT_LABEL, type DockerEventApi, type DockerEvent } from "../src/deathwatch.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { ConflictError, killWorker, pauseWorker, resumeWorker, setDockerOps } from "../src/docker.js";
import { reapOnce, StallMeter } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { claimConfirm } from "../src/tasks.js";
import { telemetry } from "../src/telemetry.js";
import {
  eventually,
  events,
  makeJob,
  pullAndClaim,
  registerTestWorker,
  hybrid,
  resetState,
  silenceWorker,
  task,
  teardown,
} from "./helpers.js";

const PROJECT = "wb-test";
const cid = (n: number) => n.toString(16).padStart(12, "a"); // a 12-char "short container ID"
const fullId = (short: string) => short + "0".repeat(52);

/** Stands in for dockerode: every getEvents() call opens a new stream the test can write to. */
class FakeDocker implements DockerEventApi {
  subscriptions: Array<{ since?: string; filters: Record<string, string[]> }> = [];
  streams: PassThrough[] = [];
  containers: Array<{ Id: string; State: string }> = [];

  async getEvents(opts: { since?: string; filters: Record<string, string[]> }) {
    this.subscriptions.push(opts);
    const s = new PassThrough();
    this.streams.push(s);
    return s;
  }

  async listContainers() {
    return this.containers;
  }

  get stream() {
    return this.streams.at(-1)!;
  }

  emit(ev: DockerEvent) {
    this.stream.write(JSON.stringify(ev) + "\n");
  }
}

function dieEvent(containerShort: string, opts: { project?: string; exitCode?: string; atMs?: number; action?: string } = {}) {
  const atMs = opts.atMs ?? Date.now();
  return {
    Type: "container",
    Action: opts.action ?? "die",
    Actor: {
      ID: fullId(containerShort),
      Attributes: { [PROJECT_LABEL]: opts.project ?? PROJECT, exitCode: opts.exitCode ?? "137", name: "wb-test-detector-1" },
    },
    time: Math.floor(atMs / 1000),
    timeNano: atMs * 1_000_000,
  } satisfies DockerEvent;
}

let docker: FakeDocker;
let watch: DeathWatch;

beforeEach(async () => {
  await resetState();
  docker = new FakeDocker();
  watch = new DeathWatch(docker, PROJECT, { minReconnectMs: 10 });
});
afterEach(() => watch.stop());
afterAll(teardown);

/** A detect worker (with a realistic container ID) holding one leased task. */
async function busyWorker(n: number) {
  const { jobId } = await makeJob([`img-${n}`]);
  await dispatchOnce();
  const workerId = await registerTestWorker("detect", `host${n}`, { containerId: cid(n) });
  const lease = (await pullAndClaim(workerId, "detect"))!;
  return { jobId, workerId, taskId: lease.taskId };
}

const workerStatus = async (id: string) => (await query(`select status from workers where id = $1`, [id])).rows[0].status;

describe("docker event death watch", () => {
  it("subscribes only to die/oom events of its own Compose project", async () => {
    await watch.start();
    expect(docker.subscriptions).toEqual([
      { filters: { type: ["container"], event: ["die", "oom"], label: [`${PROJECT_LABEL}=${PROJECT}`] } },
    ]);
  });

  it("recovers a worker within milliseconds of its container's die event", async () => {
    const { workerId, taskId } = await busyWorker(1);
    const live = await registerTestWorker("detect", "live", { containerId: cid(99) });
    await watch.start();

    docker.emit(dieEvent(cid(1)));
    await eventually(async () => (await workerStatus(workerId)) === "DEAD");
    await watch.idle();

    // The task is back at the head of the queue without any dispatcher tick.
    expect(await task(taskId)).toMatchObject({ state: "PENDING", queued: hybrid, attempts: 1, lease_losses: 1 });
    if (hybrid) expect(await getRedis().lrange(keys.queue("detect"), 0, -1)).toEqual([taskId]);

    const [died] = await events("worker_died");
    expect(died.worker_id).toBe(workerId);
    expect(died.detail).toMatchObject({ via: "docker_event", exitCode: 137, dockerAction: "die" });
    expect(died.detail.detectMs).toBeGreaterThanOrEqual(0);
    expect(died.detail.detectMs).toBeLessThan(1000);

    // Recovery record: open until a live worker re-claims the task.
    let [rec] = telemetry.recoveryRecords();
    expect(rec).toMatchObject({ workerId, via: "docker_event", tasks: 1, reclaimedAt: null, killedAt: null });
    expect(rec.requeuedAt).not.toBeNull();
    await pullAndClaim(live, "detect");
    [rec] = telemetry.recoveryRecords();
    expect(rec.reclaimedAt).not.toBeNull();
    expect(rec.totalMs).toBeGreaterThanOrEqual(0);
    expect(await workerStatus(live)).toBe("ALIVE");
  });

  it("handles events split across chunks and oom events", async () => {
    const { workerId } = await busyWorker(2);
    await watch.start();
    const line = JSON.stringify(dieEvent(cid(2), { action: "oom", exitCode: "" })) + "\n";
    docker.stream.write(line.slice(0, 20));
    docker.stream.write(line.slice(20));
    await eventually(async () => (await workerStatus(workerId)) === "DEAD");
    const [died] = await events("worker_died");
    expect(died.detail).toMatchObject({ via: "docker_event", dockerAction: "oom" });
  });

  it("ignores other Compose projects' containers and unknown containers", async () => {
    const { workerId } = await busyWorker(3);
    await watch.start();
    docker.emit(dieEvent(cid(3), { project: "someone-elses-stack" })); // same ID, other project
    docker.emit(dieEvent(cid(4))); // our project, but no worker has this container
    docker.emit({ Type: "network", Action: "die", Actor: { ID: fullId(cid(3)) } });
    await new Promise((r) => setTimeout(r, 100));
    await watch.idle();
    expect(await workerStatus(workerId)).toBe("ALIVE");
    expect(await events("worker_died")).toHaveLength(0);
  });

  it("charges no attempt when the coordinator killed the worker itself", async () => {
    const { workerId, taskId } = await busyWorker(5);
    const killed: string[] = [];
    setDockerOps({ kill: async (id) => void killed.push(id) });
    await watch.start();

    await killWorker(workerId, "chaos");
    expect(killed).toEqual([cid(5)]);
    docker.emit(dieEvent(cid(5)));
    await eventually(async () => (await workerStatus(workerId)) === "DEAD");
    await watch.idle();

    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 0, lease_losses: 0, releases: 1 });
    const [reassigned] = await events("reassigned", taskId);
    expect(reassigned.detail).toMatchObject({ charged: false });
    const [died] = await events("worker_died");
    expect(died.detail.via).toBe("docker_event");
    // detectMs is measured from the kill request.
    const [rec] = telemetry.recoveryRecords();
    expect(rec.killedAt).not.toBeNull();
  });

  it("also excuses a task the worker claimed between our kill request and its death", async () => {
    const { jobId } = await makeJob(["k1", "k2"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "racer", { containerId: cid(20) });
    setDockerOps({ kill: async () => {} });
    await killWorker(w, "chaos");
    // Docker hasn't delivered the SIGKILL yet; the worker finishes nothing but claims another task.
    const late = (await pullAndClaim(w, "detect"))!;
    await silenceWorker(w);
    await reapOnce();
    expect(await task(late.taskId)).toMatchObject({ state: "PENDING", attempts: 0, releases: 1 });
    expect(jobId).toBeTruthy();
  });

  it("does not excuse a lease loss when the kill was refused", async () => {
    const { workerId, taskId } = await busyWorker(6);
    setDockerOps({
      kill: async () => {
        throw new Error("no such container");
      },
    });
    await expect(killWorker(workerId, "api")).rejects.toThrow(/no such container/);
    const { rows } = await query(`select killed_at from workers where id = $1`, [workerId]);
    expect(rows[0].killed_at).toBeNull();

    await silenceWorker(workerId);
    await reapOnce();
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 1 });
  });

  it("reconnects with since=<last event time> and ignores replayed events for re-registered workers", async () => {
    const { workerId } = await busyWorker(7);
    await watch.start();
    const at = Date.now() + 30; // later than the connection, so it becomes the replay point
    docker.emit(dieEvent(cid(7), { atMs: at }));
    await eventually(async () => (await workerStatus(workerId)) === "DEAD");
    await watch.idle();

    // The container restarts with the same hostname and registers again...
    await new Promise((r) => setTimeout(r, 50));
    await registerTestWorker("detect", "host7", { containerId: cid(7) });
    // ...then the event stream drops, and the reconnect replays the old die event.
    docker.stream.end();
    await eventually(() => docker.subscriptions.length === 2);
    expect(docker.subscriptions[1].since).toBe(dockerSince(BigInt(at) * 1_000_000n));
    docker.emit(dieEvent(cid(7), { atMs: at }));
    await new Promise((r) => setTimeout(r, 100));
    await watch.idle();

    expect(await workerStatus(workerId)).toBe("ALIVE");
    expect(await events("worker_died")).toHaveLength(1);
  });

  it("reconciles against docker ps: an exited container's worker is recovered", async () => {
    const { workerId, taskId } = await busyWorker(8);
    const running = await registerTestWorker("detect", "running", { containerId: cid(9) });
    docker.containers = [
      { Id: fullId(cid(8)), State: "exited" },
      { Id: fullId(cid(9)), State: "running" },
    ];
    await watch.start(); // every (re)connect reconciles
    await watch.idle();

    expect(await workerStatus(workerId)).toBe("DEAD");
    expect(await workerStatus(running)).toBe("ALIVE");
    expect(await task(taskId)).toMatchObject({ state: "PENDING", queued: hybrid });
    const [died] = await events("worker_died");
    expect(died.detail).toMatchObject({ via: "docker_event", reconciled: true });
  });

  it("the heartbeat backstop still works and says so", async () => {
    const { workerId } = await busyWorker(10);
    await silenceWorker(workerId, 10_000);
    const r = await reapOnce();
    expect(r.dead).toEqual([workerId]);
    const [died] = await events("worker_died");
    expect(died.detail).toMatchObject({ via: "heartbeat" });
    expect(died.detail.detectMs).toBeGreaterThanOrEqual(10_000);
  });
});

describe("pause", () => {
  it("freezes a container, and a task lost to the pause costs no attempt", async () => {
    const { workerId, taskId } = await busyWorker(11);
    const calls: string[] = [];
    setDockerOps({
      pause: async (id) => void calls.push(`pause ${id}`),
      unpause: async (id) => void calls.push(`unpause ${id}`),
    });
    const res = await pauseWorker(workerId, 60_000);
    expect(res.ok).toBe(true);
    expect(calls).toEqual([`pause ${cid(11)}`]);
    await expect(pauseWorker(workerId, 1000)).rejects.toMatchObject({ code: "ALREADY_PAUSED" });

    // Frozen past the heartbeat timeout: declared dead, task reassigned for free.
    await silenceWorker(workerId);
    await reapOnce();
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 0, releases: 1 });

    expect(await resumeWorker(workerId)).toBe(true);
    expect(calls).toEqual([`pause ${cid(11)}`, `unpause ${cid(11)}`]);
    const { rows } = await query(`select paused_until - paused_at as d from workers where id = $1`, [workerId]);
    expect(rows[0].d).toMatchObject({ minutes: 1 });
    expect((await events("worker_paused"))[0].detail).toMatchObject({ ms: 60_000 });
    expect(await events("worker_resumed")).toHaveLength(1);
  });

  it("charges a genuine lease loss of a task started after the pause ended", async () => {
    const { jobId } = await makeJob(["p1"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "paused-before", { containerId: cid(21) });
    setDockerOps({ pause: async () => {}, unpause: async () => {} });
    await pauseWorker(w, 1000);
    await resumeWorker(w);
    await query(`update workers set paused_until = now() - interval '1 second' where id = $1`, [w]); // long over
    const lease = (await pullAndClaim(w, "detect"))!;
    await query(`update tasks set lease_expires_at = now() - interval '1 second' where id = $1`, [lease.taskId]);
    await reapOnce();
    expect(await task(lease.taskId)).toMatchObject({ state: "PENDING", attempts: 1, lease_losses: 1 });
    expect(jobId).toBeTruthy();
  });

  it("unpauses on its own after ms", async () => {
    const w = await registerTestWorker("detect", "brief", { containerId: cid(12) });
    const calls: string[] = [];
    setDockerOps({ pause: async () => void calls.push("pause"), unpause: async () => void calls.push("unpause") });
    await pauseWorker(w, 50);
    await eventually(() => calls.includes("unpause"));
    await eventually(async () => (await events("worker_resumed")).length === 1);
  });

  it("refuses native workers (409 NOT_A_CONTAINER) and bad durations", async () => {
    const native = await registerTestWorker("detect", "mac", { containerId: "native-mac", runtime: "native" });
    await expect(pauseWorker(native, 1000)).rejects.toBeInstanceOf(ConflictError);
    await expect(killWorker(native, "api")).rejects.toMatchObject({ code: "NOT_A_CONTAINER" });
    const w = await registerTestWorker("detect", "c", { containerId: cid(13) });
    await expect(pauseWorker(w, 0)).rejects.toThrow(/ms must be/);
    await expect(pauseWorker(w, "soon")).rejects.toThrow(/ms must be/);
  });
});

describe("reaper self-awareness", () => {
  it("measures its own lateness and turns recent stalls into grace", () => {
    const m = new StallMeter(1000, 6000, 250);
    expect(m.tick(0)).toBe(0);
    expect(m.tick(1100)).toBe(0); // on time (within slack)
    expect(m.tick(5100)).toBe(2750); // 4 s gap = 3 s late - 250 ms slack
    expect(m.graceMs(5100)).toBe(2750);
    expect(m.tick(6200)).toBe(0);
    expect(m.graceMs(10_000)).toBe(2750);
    expect(m.graceMs(11_200)).toBe(0); // the stall has aged out of the window
  });

  it("does not convict workers for a stall of its own", async () => {
    const { workerId, taskId } = await busyWorker(14);
    // Silent for 3 s against a 2 s timeout, but the reaper itself was stalled for 5 s.
    await silenceWorker(workerId, 3000);
    await query(`update tasks set lease_expires_at = now() - interval '1 second' where id = $1`, [taskId]);
    expect((await reapOnce(5000)).dead).toEqual([]);
    expect(await task(taskId)).toMatchObject({ state: "LEASED" });
    expect((await reapOnce(0)).dead).toEqual([workerId]);
  });
});

describe("claims after recovery", () => {
  it("a recovered task is re-leased with a higher epoch", async () => {
    const { workerId, taskId } = await busyWorker(15);
    await watch.start();
    docker.emit(dieEvent(cid(15)));
    await eventually(async () => (await workerStatus(workerId)) === "DEAD");
    await watch.idle();
    const other = await registerTestWorker("detect", "other", { containerId: cid(16) });
    const [lease] = await claimConfirm(other, [taskId]);
    expect(lease.leaseEpoch).toBe(2);
  });
});
