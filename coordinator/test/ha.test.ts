// Coordinator HA (docs/decisions/h-ha.md): leader election with term fencing, what only the leader
// does, and the cluster bus. Electors are driven round by round (tick()) instead of on timers.
import { PassThrough } from "node:stream";
import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { busChannel, ClusterBus } from "../src/cluster.js";
import { config } from "../src/config.js";
import { FencedError, query, tx, withFence } from "../src/db.js";
import { DeathWatch, PROJECT_LABEL, type DockerEventApi } from "../src/deathwatch.js";
import { dispatchOnce, isThrottled, loadThrottle, rebuildQueues, repairLostQueued } from "../src/dispatcher.js";
import { hub, type EventRow } from "../src/events.js";
import { startHa, type LeaderWork } from "../src/ha.js";
import { Elector, leaderView, type Election } from "../src/leader.js";
import { reconcileAsLeader } from "../src/loops.js";
import { reapOnce } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { systemSnapshot } from "../src/system.js";
import { telemetry, type TimingSample } from "../src/telemetry.js";
import {
  eventually,
  events,
  hybrid,
  makeJob,
  registerTestWorker,
  resetState,
  silenceWorker,
  tasksOfJob,
  teardown,
} from "./helpers.js";

async function resetLeader() {
  await query(
    `update coordinator_leader set holder = null, instance = null, term = 0, since = null, renewed_at = null,
            expires_at = '-infinity', resigned_at = null, throttled = false, chaos_enabled = false, chaos_every_sec = 20`,
  );
  await query(`delete from coordinator_nodes`);
}

/** Pretends the current leader stopped renewing long enough ago that its lease ran out. */
async function expireLease() {
  await query(`update coordinator_leader set expires_at = now() - interval '1 millisecond'`);
}

const electors: Elector[] = [];
function elector(id: string, opts: Partial<{ ttlMs: number; renewMs: number }> = {}) {
  const log = { elected: [] as Election[], deposed: [] as string[] };
  const e = new Elector(id, {
    ttlMs: opts.ttlMs ?? 5000,
    renewMs: opts.renewMs ?? 1000,
    onElected: (x) => void log.elected.push(x),
    onDeposed: (reason) => void log.deposed.push(reason),
  });
  electors.push(e);
  return Object.assign(e, { log });
}

beforeEach(async () => {
  await resetState();
  await resetLeader();
});

afterEach(async () => {
  for (const e of electors.splice(0)) await e.stop();
});

afterAll(teardown);

describe("leader election", () => {
  it("elects exactly one leader; renewals keep the term", async () => {
    const a = elector("coord-a");
    const b = elector("coord-b");
    await a.tick();
    await b.tick();
    expect(a.isLeader()).toBe(true);
    expect(b.isLeader()).toBe(false);
    expect(a.log.elected.map((e) => e.fence.term)).toEqual([1]);
    expect(a.log.elected[0].previous).toBeNull();
    expect(b.log.elected).toEqual([]);

    const before = (await query(`select expires_at from coordinator_leader`)).rows[0].expires_at;
    await new Promise((r) => setTimeout(r, 20));
    await a.tick();
    await b.tick();
    const row = (await query(`select holder, term, expires_at from coordinator_leader`)).rows[0];
    expect(row).toMatchObject({ holder: "coord-a", term: "1" });
    expect(new Date(row.expires_at).getTime()).toBeGreaterThan(new Date(before).getTime());
    expect(a.log.elected).toHaveLength(1);
    expect(b.view).toMatchObject({ holder: "coord-a", term: 1 });

    const nodes = (await query(`select id, role, term from coordinator_nodes order by id`)).rows;
    expect(nodes).toEqual([
      { id: "coord-a", role: "leader", term: "1" },
      { id: "coord-b", role: "follower", term: null },
    ]);
  });

  it("a takeover after the lease lapses bumps the term, and the old leader steps down on its next renewal", async () => {
    const a = elector("coord-a");
    const b = elector("coord-b");
    await a.tick();
    await b.tick();
    await expireLease(); // a was paused past its lease
    await b.tick();
    expect(b.isLeader()).toBe(true);
    expect(b.log.elected[0].fence.term).toBe(2);
    expect(b.log.elected[0].previous).toMatchObject({ holder: "coord-a", term: 1, resigned: false });

    expect(a.isLeader()).toBe(true); // it doesn't know yet
    await a.tick();
    expect(a.isLeader()).toBe(false);
    expect(a.log.deposed).toEqual(["superseded: another replica holds a newer term"]);
    expect(a.log.elected).toHaveLength(1); // and it can't take the lease back
  });

  it("a resigning leader hands over at once, without waiting for the TTL", async () => {
    const a = elector("coord-a", { ttlMs: 60_000 });
    const b = elector("coord-b", { ttlMs: 60_000 });
    await a.tick();
    await b.tick();
    await a.stop();
    await b.tick();
    expect(b.isLeader()).toBe(true);
    expect(b.log.elected[0].previous).toMatchObject({ holder: "coord-a", term: 1, resigned: true });
  });

  it("the same COORDINATOR_ID restarted is a different instance: it waits for its old lease like anyone else", async () => {
    const old = elector("coord-a");
    await old.tick();
    const restarted = elector("coord-a");
    await restarted.tick();
    expect(restarted.isLeader()).toBe(false);
    await expireLease();
    await restarted.tick();
    expect(restarted.log.elected[0].fence.term).toBe(2);
  });

  it("steps down when it cannot renew for longer than TTL minus one tick", async () => {
    const a = elector("coord-a", { ttlMs: 300, renewMs: 100 });
    await a.tick();
    expect(a.isLeader()).toBe(true);
    // Break the election connection: every round fails until the renew deadline passes.
    (a as any).connection = async () => {
      throw new Error("connection refused");
    };
    await a.tick();
    expect(a.isLeader()).toBe(true); // one failed renewal is not enough
    await new Promise((r) => setTimeout(r, 250));
    await a.tick();
    expect(a.isLeader()).toBe(false);
    expect(a.log.deposed[0]).toMatch(/could not renew the lease/);
  });
});

describe("term fencing", () => {
  /** a leads term 1, then b takes over term 2 while a still believes it leads. */
  async function deposedLeader() {
    const a = elector("coord-a");
    const b = elector("coord-b");
    await a.tick();
    const stale = a.currentFence()!;
    await expireLease();
    await b.tick();
    return { a, b, stale, current: b.currentFence()! };
  }

  it("rejects a deposed leader's reaper pass: nothing is written, the old leader steps down, the rejection is recorded", async () => {
    const workerId = await registerTestWorker("detect", "zombie-victim");
    await silenceWorker(workerId);
    const { a, stale, current } = await deposedLeader();

    await expect(withFence(stale, () => reapOnce())).rejects.toBeInstanceOf(FencedError);
    expect((await query(`select status from workers where id = $1`, [workerId])).rows[0].status).toBe("ALIVE");
    expect(a.isLeader()).toBe(false);
    expect(a.log.deposed[0]).toMatch(/fenced: term 2 is current/);
    expect(a.stats.fencedRejections).toBe(1);
    const fenced = await eventually(async () => (await events("leader_fenced"))[0]);
    expect(fenced.detail).toMatchObject({ holder: "coord-a", term: 1, currentTerm: 2, currentHolder: "coord-b" });
    expect(fenced.leader_term).toBeNull();

    // The current leader's pass goes through, and its writes carry its term.
    const r = await withFence(current, () => reapOnce());
    expect(r.dead).toEqual([workerId]);
    const died = await events("worker_died");
    expect(died.map((e) => e.leader_term)).toEqual(["2"]);
  });

  it("rejects a deposed leader's dispatcher sweep and throttle decision", async () => {
    const { stale } = await deposedLeader();
    await makeJob(["a", "b", "c", "d", "e", "f"]);
    await expect(withFence(stale, () => dispatchOnce())).rejects.toBeInstanceOf(FencedError);
    await expect(withFence(stale, () => rebuildQueues("stale"))).rejects.toBeInstanceOf(FencedError);
    expect((await query(`select throttled from coordinator_leader`)).rows[0].throttled).toBe(false);
  });

  it("a guarded transaction holds a takeover off until it commits, so terms never overlap", async () => {
    const a = elector("coord-a");
    const b = elector("coord-b", { renewMs: 3000 }); // lock_timeout long enough to wait it out
    await a.tick();
    const stale = a.currentFence()!;
    await expireLease();
    let takeoverDone = 0;
    const slow = withFence(stale, () =>
      tx(async (c) => {
        await c.query(`insert into task_events (type) values ('probe_old_term')`);
        await c.query(`select pg_sleep(0.4)`);
        return Date.now();
      }),
    );
    await new Promise((r) => setTimeout(r, 50)); // the guard (FOR SHARE) is held by now
    const takeover = b.tick().then(() => (takeoverDone = Date.now()));
    const committedAt = await slow;
    await takeover;
    expect(b.currentFence()?.term).toBe(2);
    expect(takeoverDone).toBeGreaterThanOrEqual(committedAt);
    await withFence(b.currentFence()!, () => query(`insert into task_events (type) values ('probe_new_term')`));
    // The old term's write is the earlier one and carries the old term.
    const { rows } = await query(`select type, leader_term from task_events where type like 'probe%' order by id`);
    expect(rows).toEqual([
      { type: "probe_old_term", leader_term: "1" },
      { type: "probe_new_term", leader_term: "2" },
    ]);
    // Terms never go backwards in insertion order (the failover test's stale-write check).
    const { rows: bad } = await query(
      `select count(*)::int as n from (
         select leader_term, max(leader_term) over (order by id rows between unbounded preceding and 1 preceding) as prev
           from task_events where leader_term is not null) x
        where leader_term < prev`,
    );
    expect(bad[0].n).toBe(0);
  });

  it("a session the server ends mid-transaction fails that transaction instead of crashing the process", async () => {
    // What a frozen leader sees on waking: its guarded transaction's session was ended by the
    // idle-in-transaction timeout (or pg_terminate_backend) while it was paused.
    await expect(
      tx(async (c) => {
        const { rows } = await c.query("select pg_backend_pid() as pid");
        await query("select pg_terminate_backend($1)", [rows[0].pid]);
        await new Promise((r) => setTimeout(r, 100)); // the FATAL arrives while no statement is running
        await c.query("select 1");
      }),
    ).rejects.toThrow();
    expect((await query("select 1 as x")).rows[0].x).toBe(1);
  });

  it("rejects a death-watch reaction that arrives after the watch's leader was deposed", async () => {
    const workerId = await registerTestWorker("detect", "d1", { containerId: "abcabcabcabc" });
    const { stale } = await deposedLeader();
    const stream = new PassThrough();
    const docker: DockerEventApi = {
      getEvents: async () => stream,
      listContainers: async () => [],
    };
    const watch = new DeathWatch(docker, "wb-test");
    await withFence(stale, () => watch.start());
    stream.write(
      JSON.stringify({
        Type: "container",
        Action: "die",
        Actor: { ID: "abcabcabcabc" + "0".repeat(52), Attributes: { [PROJECT_LABEL]: "wb-test", exitCode: "137" } },
        timeNano: Date.now() * 1e6,
      }) + "\n",
    );
    await new Promise((r) => setTimeout(r, 100));
    await watch.idle();
    watch.stop();
    expect((await query(`select status from workers where id = $1`, [workerId])).rows[0].status).toBe("ALIVE");
    await eventually(async () => (await events("leader_fenced")).length > 0);
  });
});

describe("leader-only work", () => {
  function fakeWork() {
    const log: string[] = [];
    const work: LeaderWork = {
      async lead(e, _elector, onStop) {
        log.push(`start ${e.fence.holder} term ${e.fence.term}`);
        onStop(() => log.push(`stop ${e.fence.holder} term ${e.fence.term}`));
      },
    };
    return { log, work };
  }

  it("runs only on the leader, stops when deposed, and moves to the follower on resignation", async () => {
    const saved = { ttl: config.leaderTtlMs, renew: config.leaderRenewMs };
    config.leaderTtlMs = 60_000;
    config.leaderRenewMs = 50;
    const { log, work } = fakeWork();
    const a = await startHa({ id: "coord-a", work, bus: false, replicaLoops: false });
    const b = await startHa({ id: "coord-b", work, bus: false, replicaLoops: false });
    try {
      await eventually(() => log.length === 1);
      expect(a.elector.isLeader()).toBe(true);
      expect(b.elector.isLeader()).toBe(false);
      await new Promise((r) => setTimeout(r, 200)); // several rounds: still only a
      expect(log).toEqual(["start coord-a term 1"]);

      await a.stop(); // resigns
      await eventually(() => b.elector.isLeader());
      await eventually(() => log.length === 3);
      expect(log).toEqual(["start coord-a term 1", "stop coord-a term 1", "start coord-b term 2"]);
    } finally {
      await a.stop();
      await b.stop();
      config.leaderTtlMs = saved.ttl;
      config.leaderRenewMs = saved.renew;
    }
  });

  it.skipIf(!hybrid)("a follower starting up leaves the queues alone; the election rebuilds them", async () => {
    // Someone else leads for the next minute.
    await query(
      `update coordinator_leader set holder = 'other', instance = 'x', term = 7, since = now(), renewed_at = now(),
              expires_at = now() + interval '1 minute'`,
    );
    const { jobId } = await makeJob(["q1", "q2", "q3"]);
    await eventually(async () => (await getRedis().llen(keys.queue("detect"))) === 3);
    await getRedis().rpush(keys.queue("detect"), "not-a-real-task");

    const saved = { ttl: config.leaderTtlMs, renew: config.leaderRenewMs, events: config.dockerEvents };
    config.leaderRenewMs = 50;
    config.dockerEvents = "off";
    const node = await startHa({ id: "coord-f", bus: false, replicaLoops: false });
    try {
      await new Promise((r) => setTimeout(r, 200));
      expect(node.elector.isLeader()).toBe(false);
      expect(await getRedis().llen(keys.queue("detect"))).toBe(4); // untouched
      expect((await tasksOfJob(jobId)).every((t) => t.queued)).toBe(true);

      await expireLease();
      const elected = await eventually(async () => (await events("leader_elected"))[0], 5000);
      expect(elected.detail).toMatchObject({ holder: "coord-f", term: 8, previousHolder: "other", previousTerm: 7 });
      expect(elected.leader_term).toBe("8");
      expect((await events("leader_lost"))[0].detail).toMatchObject({ holder: "other", term: 7, reason: "lease expired" });
      // Rebuilt: the stray ID is gone, the three real tasks are queued once each.
      const queue = await getRedis().lrange(keys.queue("detect"), 0, -1);
      expect(queue.sort()).toEqual((await tasksOfJob(jobId)).map((t) => t.id).sort());
      expect(leaderView()).toMatchObject({ id: "coord-f", term: 8 });
      expect((await systemSnapshot()).leader).toMatchObject({ id: "coord-f", term: 8 });
    } finally {
      await node.stop();
      config.leaderTtlMs = saved.ttl;
      config.leaderRenewMs = saved.renew;
      config.dockerEvents = saved.events;
    }
  });

  it("grants start-up grace only on a cold start (nobody processed a heartbeat for two intervals)", async () => {
    const warm = await registerTestWorker("detect", "warm");
    await reconcileAsLeader("me", 1);
    const cold = await registerTestWorker("detect", "cold");
    await silenceWorker(warm, 60_000);
    await silenceWorker(cold, 60_000);
    const r = await reconcileAsLeader("me", 1);
    expect(r.coldStart).toBe(true);
    const { rows } = await query(`select count(*)::int as n from workers where last_heartbeat_at > now() - interval '1 second'`);
    expect(rows[0].n).toBe(2);

    await silenceWorker(cold, 60_000); // warm heartbeated just now (the grace): a failover, not a cold start
    const again = await reconcileAsLeader("me", 1);
    expect(again.coldStart).toBe(false);
    expect((await query(`select last_heartbeat_at < now() - interval '30 seconds' as old from workers where id = $1`, [cold])).rows[0].old).toBe(true);
  });

  it.skipIf(!hybrid)("the queued-row audit re-dispatches IDs that were popped and lost, and nothing else", async () => {
    const { jobId } = await makeJob(["l1", "l2", "l3"]);
    const queue = keys.queue("detect");
    await eventually(async () => (await getRedis().llen(queue)) === 3);
    const [lost, moved] = await getRedis().lrange(queue, 0, 1);
    await getRedis().lrem(queue, 0, lost); // popped by a claim-next whose lease statement then failed
    await getRedis().lrem(queue, 0, moved);
    await registerTestWorker("detect", "w");
    await getRedis().rpush(keys.processing("detect-w"), moved); // a worker's BLMOVE: still accounted for
    expect(await repairLostQueued()).toBe(0); // too recent to judge
    expect(await repairLostQueued(0)).toBe(1);
    const byId = new Map((await tasksOfJob(jobId)).map((t) => [t.id, t]));
    expect(byId.get(lost).queued).toBe(false);
    expect(byId.get(moved).queued).toBe(true);
    await dispatchOnce(); // the repair sweep pushes it again
    expect(await getRedis().lrange(queue, 0, -1)).toContain(lost);
  });

  it("the throttle flag the leader decides reaches the followers through Postgres", async () => {
    await query(`update coordinator_leader set throttled = true`);
    expect(isThrottled()).toBe(false);
    await loadThrottle();
    expect(isThrottled()).toBe(true);
    await query(`update coordinator_leader set throttled = false`);
    await loadThrottle();
    expect(isThrottled()).toBe(false);
  });
});

describe("cluster bus", () => {
  const sample = (): TimingSample => ({
    at: Date.now(),
    stage: "detect",
    serviceMs: 5,
    dispatchWaitMs: 1,
    queueWaitMs: 2,
    claimMs: 1,
    fetchMs: 0,
    inferMs: 3,
    uploadMs: 0,
    completeMs: 1,
    totalMs: 8,
  });

  it("relays hub notifications and telemetry, applies a peer's without echoing, ignores its own", async () => {
    const bus = new ClusterBus("me");
    const listener = getRedis().duplicate();
    const got: any[] = [];
    listener.on("message", (_c: string, m: string) => {
      const msg = JSON.parse(m);
      if (msg.from === "me") got.push(msg);
    });
    await listener.subscribe(busChannel());
    await bus.start();
    try {
      const row: EventRow = { id: 1, at: new Date().toISOString(), type: "worker_died", taskId: null, workerId: "w", detail: {} };
      hub.publishEvents([row, { ...row, id: 2, type: "claimed" }]); // bookkeeping types stay local
      hub.jobChanged("job-1");
      hub.workersChanged();
      telemetry.recordCompletion(sample(), "00000000-0000-0000-0000-000000000001");
      await eventually(() => got.length > 0);
      expect(got[0]).toMatchObject({ from: "me", events: [{ type: "worker_died" }], jobs: ["job-1"], workers: true });
      expect(got[0].events).toHaveLength(1);
      expect(got[0].telemetry[0][0]).toBe("recordSample");

      // A peer's completion: counted in this replica's telemetry, but not in its complete_ms write-behind.
      telemetry.drainCompleteMs();
      const before = telemetry.timings().samples;
      bus.receive(JSON.stringify({ from: "peer", telemetry: [["recordSample", [sample()]]], workers: true }));
      expect(telemetry.timings().samples).toBe(before + 1);
      expect(telemetry.drainCompleteMs()).toEqual([]);
      bus.receive(JSON.stringify({ from: "me", telemetry: [["recordSample", [sample()]]] }));
      expect(telemetry.timings().samples).toBe(before + 1);
      await new Promise((r) => setTimeout(r, 120));
      expect(got).toHaveLength(1); // applying the peer's message relayed nothing
    } finally {
      await bus.stop();
      await listener.quit();
    }
    expect(Object.hasOwn(telemetry, "recordCompletion")).toBe(false); // unwrapped again
  });

  it("relays a recovery and the claim that closes it, so every replica's snapshot shows it", async () => {
    const bus = new ClusterBus("me");
    await bus.start();
    try {
      const now = new Date();
      bus.receive(
        JSON.stringify({
          from: "leader",
          telemetry: [
            ["recordRecovery", [{ workerId: "w1", killedAt: null, startMs: now.getTime() - 500, detectedAt: now.toISOString(), via: "docker_event", requeuedAt: now.toISOString(), taskIds: ["t1"] }]],
          ],
        }),
      );
      expect(telemetry.recoveryRecords()[0]).toMatchObject({ workerId: "w1", tasks: 1, reclaimedAt: null });
      expect(telemetry.isRecovering("t1")).toBe(true);
      bus.receive(JSON.stringify({ from: "peer", telemetry: [["recordClaimed", [["t1"], "w2", now.getTime() + 100]]] }));
      expect(telemetry.recoveryRecords()[0]).toMatchObject({ reclaimedBy: "w2", totalMs: 600 });
    } finally {
      await bus.stop();
    }
  });
});
