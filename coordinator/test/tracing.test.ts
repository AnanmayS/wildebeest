// Observability: per-image traces (otel.ts) and the Prometheus endpoint (prom.ts).
// Runs in both claim modes (`npm test`). The SDK is installed once for the file with an in-memory
// exporter; "tracing off" switches the hooks off with the SDK still present, so a hook that forgot
// its guard would show up as a span.
import http from "node:http";
import type { AddressInfo } from "node:net";
import { SpanKind, SpanStatusCode, type Context, type Attributes } from "@opentelemetry/api";
import {
  InMemorySpanExporter,
  ParentBasedSampler,
  SamplingDecision,
  type ReadableSpan,
  type Sampler,
} from "@opentelemetry/sdk-trace-node";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { query } from "../src/db.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { createSyntheticJob } from "../src/jobs.js";
import { parseTraceparent, resetTraceState } from "../src/otel.js";
import { setTracingEnabled, setupTracing, shutdownTracing } from "../src/otel-sdk.js";
import { reapOnce } from "../src/reaper.js";
import { recoverWorkers } from "../src/recovery.js";
import { getRedis, keys } from "../src/redis.js";
import { serviceTimes, speculateOnce } from "../src/speculation.js";
import { claimConfirm, claimTasks, completeTasks, type Lease } from "../src/tasks.js";
import {
  CLAIM_MODE,
  detectResult,
  postgres,
  makeJob,
  pullAndClaim,
  registerTestWorker,
  resetState,
  silenceWorker,
  task,
  tasksOfJob,
  teardown,
} from "./helpers.js";

const exporter = new InMemorySpanExporter();
/** Root sampling switch: the tests flip it to check that unsampled traces stay unsampled. */
let sampleRoots = true;
const rootSampler: Sampler = {
  shouldSample: (_ctx: Context, _traceId: string, _name: string, _kind: SpanKind, _attrs: Attributes) => ({
    decision: sampleRoots ? SamplingDecision.RECORD_AND_SAMPLED : SamplingDecision.NOT_RECORD,
  }),
  toString: () => "TestRootSampler",
};

let server: http.Server;
let base: string;

beforeAll(async () => {
  await setupTracing({ exporter, simple: true, sampler: new ParentBasedSampler({ root: rootSampler }) });
  server = http.createServer(createApp());
  await new Promise<void>((resolve) => server.listen(0, resolve));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => {
  await new Promise((r) => server.close(r));
  await shutdownTracing();
  await teardown();
});
beforeEach(async () => {
  await resetState();
  setTracingEnabled(true);
  sampleRoots = true;
  resetTraceState();
  exporter.reset();
});

async function post(route: string, body: unknown): Promise<{ status: number; body: any }> {
  const res = await fetch(base + route, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

const TP = /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/;
const spans = () => exporter.getFinishedSpans();
const named = (name: string, traceId?: string) =>
  spans().filter((s) => s.name === name && (!traceId || s.spanContext().traceId === traceId));
const traceOf = (tp: string) => parseTraceparent(tp)!.traceId;
const spanIdOf = (tp: string) => parseTraceparent(tp)!.spanId;
const parentOf = (s: ReadableSpan) => s.parentSpanContext?.spanId;
/** What a traced worker sends: the context of its settle span, a child of its process span. */
const workerSpan = (leaseTp: string) => `00-${traceOf(leaseTp)}-${"ab".repeat(8)}-01`;
const empty = detectResult([]);

describe(`per-image traces (CLAIM_MODE=${CLAIM_MODE})`, () => {
  it("stores the PRODUCER span's traceparent on each task and returns the attempt's context in the lease", async () => {
    const { jobId } = await makeJob(["t1", "t2"]);
    const rows = await tasksOfJob(jobId);
    expect(rows.map((r) => r.traceparent)).toEqual([expect.stringMatching(TP), expect.stringMatching(TP)]);
    expect(traceOf(rows[0].traceparent)).not.toBe(traceOf(rows[1].traceparent)); // one trace per image

    const producers = named("create detect");
    expect(producers).toHaveLength(2);
    for (const r of rows) {
      const p = producers.find((s) => s.spanContext().spanId === spanIdOf(r.traceparent))!;
      expect(p.kind).toBe(SpanKind.PRODUCER);
      expect(p.parentSpanContext).toBeUndefined(); // a root: the image's trace starts at creation
      expect(p.attributes["messaging.message.id"]).toBe(r.id);
    }

    const w = await registerTestWorker("detect", "w1");
    const lease = (await pullAndClaim(w, "detect"))!;
    const row = rows.find((r) => r.id === lease.taskId)!;
    // Same trace as the row; the span is the attempt's lease span, not the producer.
    expect(lease.traceparent).toMatch(TP);
    expect(traceOf(lease.traceparent!)).toBe(traceOf(row.traceparent));
    expect(spanIdOf(lease.traceparent!)).not.toBe(spanIdOf(row.traceparent));
    expect((await task(lease.taskId)).traceparent).toBe(row.traceparent); // the row keeps the creation context

    const settle = workerSpan(lease.traceparent!);
    const res = await post(`/tasks/${lease.taskId}/complete`, {
      workerId: w,
      leaseEpoch: lease.leaseEpoch,
      result: empty,
      traceparent: settle,
    });
    expect(res.status).toBe(200);

    const trace = traceOf(row.traceparent);
    const [leaseSpan] = named("lease detect", trace);
    expect(leaseSpan.parentSpanContext?.spanId).toBe(spanIdOf(row.traceparent));
    expect(leaseSpan.spanContext().spanId).toBe(spanIdOf(lease.traceparent!));
    expect(leaseSpan.attributes["wildebeest.lease_epoch"]).toBe(1);
    expect(leaseSpan.attributes["wildebeest.worker_id"]).toBe(w);
    expect(leaseSpan.events.map((e) => e.name)).toContain("completed");
    const [complete] = named("complete detect", trace);
    expect(parentOf(complete)).toBe(spanIdOf(settle)); // under the worker's settle span
    expect(complete.attributes["wildebeest.write.accepted"]).toBe(true);
  });

  it("attributes each task of a batched complete to its own trace", async () => {
    const { jobId } = await makeJob(["b1", "b2", "b3"]);
    const w = await registerTestWorker("detect", "w1");
    const leases = [(await pullAndClaim(w, "detect"))!, (await pullAndClaim(w, "detect"))!];
    const res = await post("/tasks/complete-batch", {
      workerId: w,
      items: leases.map((l) => ({ taskId: l.taskId, leaseEpoch: l.leaseEpoch, result: empty, traceparent: workerSpan(l.traceparent!) })),
    });
    expect(res.body.results.map((r: any) => r.status)).toEqual(["ok", "ok"]);

    const rows = await tasksOfJob(jobId);
    for (const l of leases) {
      const trace = traceOf(rows.find((r) => r.id === l.taskId)!.traceparent);
      const complete = named("complete detect", trace);
      expect(complete).toHaveLength(1);
      expect(complete[0].attributes["messaging.message.id"]).toBe(l.taskId);
      expect(complete[0].attributes["wildebeest.batch_size"]).toBe(2);
      expect(named("lease detect", trace)).toHaveLength(1); // attempt closed
    }
    // A batch without worker contexts still lands in the right traces (under the lease span).
    const third = (await pullAndClaim(w, "detect"))!;
    await completeTasks(w, [{ taskId: third.taskId, leaseEpoch: third.leaseEpoch, result: empty }]);
    const [c3] = named("complete detect", traceOf(third.traceparent!));
    expect(parentOf(c3)).toBe(spanIdOf(third.traceparent!));
  });

  it("keeps a reclaimed task in one trace: lost attempt, requeue, higher-epoch attempt, fenced zombie", async () => {
    const { jobId } = await makeJob(["k1"]);
    const [row] = await tasksOfJob(jobId);
    const trace = traceOf(row.traceparent);
    const w1 = await registerTestWorker("detect", "w1");
    const w2 = await registerTestWorker("detect", "w2");
    const first = (await pullAndClaim(w1, "detect"))!;
    expect(first.leaseEpoch).toBe(1);

    await recoverWorkers([w1], "docker_event"); // SIGKILLed: the death watch requeues its lease
    const second = (await pullAndClaim(w2, "detect"))!;
    expect(second.taskId).toBe(first.taskId);
    expect(second.leaseEpoch).toBe(2);
    expect(traceOf(second.traceparent!)).toBe(trace);

    // The zombie's late result is fenced off, then the new owner completes.
    const zombie = await post(`/tasks/${first.taskId}/complete`, {
      workerId: w1, leaseEpoch: 1, result: empty, traceparent: workerSpan(first.traceparent!),
    });
    expect(zombie.status).toBe(409);
    await post(`/tasks/${second.taskId}/complete`, {
      workerId: w2, leaseEpoch: 2, result: empty, traceparent: workerSpan(second.traceparent!),
    });

    const inTrace = spans().filter((s) => s.spanContext().traceId === trace);
    const leasesInTrace = inTrace.filter((s) => s.name === "lease detect").sort((a, b) =>
      Number(a.attributes["wildebeest.lease_epoch"]) - Number(b.attributes["wildebeest.lease_epoch"]));
    expect(leasesInTrace.map((s) => s.attributes["wildebeest.lease_epoch"])).toEqual([1, 2]);
    const [lost, won] = leasesInTrace;
    expect(lost.status.code).toBe(SpanStatusCode.ERROR);
    expect(lost.status.message).toContain("docker_event");
    expect(lost.events.map((e) => e.name)).toContain("reassigned");
    expect(won.status.code).not.toBe(SpanStatusCode.ERROR);
    expect(won.events.map((e) => e.name)).toEqual(["completed"]);
    // Attempt spans have IDs derived from (task, epoch): the worker's context is the lease span.
    expect(lost.spanContext().spanId).toBe(spanIdOf(first.traceparent!));
    expect(won.spanContext().spanId).toBe(spanIdOf(second.traceparent!));

    const [requeue] = inTrace.filter((s) => s.name === "requeue detect");
    expect(parentOf(requeue)).toBe(spanIdOf(row.traceparent));
    expect(requeue.attributes).toMatchObject({ "wildebeest.reason": "worker_dead", "wildebeest.death.via": "docker_event" });
    // Lost attempt ended before the requeue; the new attempt started after it.
    const ms = (t: [number, number]) => t[0] * 1e3 + t[1] / 1e6;
    // (span clocks are anchored per trace root, so allow a few ms of skew)
    expect(ms(lost.endTime)).toBeLessThanOrEqual(ms(requeue.endTime) + 5);
    expect(ms(requeue.startTime)).toBeLessThanOrEqual(ms(won.startTime) + 5);

    const completes = inTrace.filter((s) => s.name === "complete detect");
    expect(completes.map((s) => s.attributes["wildebeest.write.accepted"]).sort()).toEqual([false, true]);
    expect(completes.find((s) => !s.attributes["wildebeest.write.accepted"])!.status.code).toBe(SpanStatusCode.ERROR);
  });

  it("requeues after a coordinator restart still land in the task's trace (context read from the row)", async () => {
    const { jobId } = await makeJob(["r1"]);
    const [row] = await tasksOfJob(jobId);
    const w1 = await registerTestWorker("detect", "w1");
    await pullAndClaim(w1, "detect");
    resetTraceState(); // the process that opened the lease span is gone
    await silenceWorker(w1);
    await reapOnce();
    const requeue = await (async () => {
      for (let i = 0; i < 50; i++) {
        const r = named("requeue detect", traceOf(row.traceparent));
        if (r.length) return r[0];
        await new Promise((res) => setTimeout(res, 20));
      }
      throw new Error("no requeue span");
    })();
    expect(requeue.attributes["wildebeest.death.via"]).toBe("heartbeat");
  });

  it("lets another replica close an attempt (HA): span IDs derive from (task, epoch), context from the row", async () => {
    const { jobId } = await makeJob(["h1"]);
    const [row] = await tasksOfJob(jobId);
    const w = await registerTestWorker("detect", "w1");
    const lease = (await pullAndClaim(w, "detect"))!;
    resetTraceState(); // the complete lands on a replica that never saw the claim; the worker is untraced
    await completeTasks(w, [{ taskId: lease.taskId, leaseEpoch: lease.leaseEpoch, result: empty }]);
    const [leaseSpan] = named("lease detect", traceOf(row.traceparent));
    expect(leaseSpan.spanContext().spanId).toBe(spanIdOf(lease.traceparent!));
    expect(leaseSpan.parentSpanContext?.spanId).toBe(spanIdOf(row.traceparent));
    const [complete] = named("complete detect", traceOf(row.traceparent));
    expect(parentOf(complete)).toBe(spanIdOf(lease.traceparent!));
    // Starts at the claim (tasks.started_at), not when this replica heard about it.
    const started = new Date((await task(lease.taskId)).started_at).getTime();
    expect(Math.abs(leaseSpan.startTime[0] * 1e3 + leaseSpan.startTime[1] / 1e6 - started)).toBeLessThan(5);
  });

  it("puts the classify task a detect completion creates into the same image trace", async () => {
    const { jobId } = await makeJob(["c1"]);
    const [row] = await tasksOfJob(jobId);
    const w = await registerTestWorker("detect", "w1");
    const lease = (await pullAndClaim(w, "detect"))!;
    await post(`/tasks/${lease.taskId}/complete`, {
      workerId: w, leaseEpoch: 1, result: detectResult([{ label: "animal", conf: 0.9 }]), traceparent: workerSpan(lease.traceparent!),
    });
    const classify = (await tasksOfJob(jobId)).find((t) => t.stage === "classify")!;
    expect(classify.traceparent).toMatch(TP);
    const trace = traceOf(row.traceparent);
    expect(traceOf(classify.traceparent)).toBe(trace);
    const [create] = named("create classify", trace);
    const [complete] = named("complete detect", trace);
    expect(create.spanContext().spanId).toBe(spanIdOf(classify.traceparent));
    expect(parentOf(create)).toBe(complete.spanContext().spanId);

    const cw = await registerTestWorker("classify", "c1");
    const cl = (await pullAndClaim(cw, "classify"))!;
    expect(traceOf(cl.traceparent!)).toBe(trace);
  });

  it("gives a speculative copy its own attempt span in the task's trace; the loser ends as lost_race", async () => {
    config.speculateMinSamples = 5;
    config.speculateMultiplier = 3;
    config.speculateMinMs = 1000;
    const { jobId } = await makeJob(["s1"]);
    await dispatchOnce();
    const [row] = await tasksOfJob(jobId);
    const trace = traceOf(row.traceparent);
    const slow = await registerTestWorker("detect", "slow");
    const original = (await pullAndClaim(slow, "detect"))!;
    const fast = await registerTestWorker("detect", "fast");
    for (let i = 0; i < 5; i++) serviceTimes.record(fast, "detect", 100);
    await query(`update tasks set started_at = now() - interval '10 seconds' where id = $1`, [original.taskId]);
    expect(await speculateOnce()).toHaveLength(1);
    let copy: Lease | null;
    if (postgres) copy = (await claimTasks(fast, "detect", 1, 0))[0] ?? null;
    else {
      const id = await getRedis().lmove(keys.spec(fast), keys.processing(fast), "LEFT", "RIGHT");
      copy = (await claimConfirm(fast, [id!]))[0] ?? null;
    }
    expect(copy).toMatchObject({ speculative: true, leaseEpoch: 2 });
    expect(traceOf(copy!.traceparent!)).toBe(trace);
    expect(spanIdOf(copy!.traceparent!)).not.toBe(spanIdOf(original.traceparent!));

    // The copy wins; the original's late report is answered already_done.
    await post(`/tasks/${copy!.taskId}/complete`, { workerId: fast, leaseEpoch: 2, result: empty, traceparent: workerSpan(copy!.traceparent!) });
    const late = await post(`/tasks/${original.taskId}/complete`, {
      workerId: slow, leaseEpoch: 1, result: empty, traceparent: workerSpan(original.traceparent!),
    });
    expect(late.status).toBe(409);

    const leases = named("lease detect", trace);
    const byEpoch = new Map(leases.map((s) => [s.attributes["wildebeest.lease_epoch"], s]));
    expect(byEpoch.get(2)!.attributes["wildebeest.speculative"]).toBe(true);
    expect(byEpoch.get(2)!.events.map((e) => e.name)).toContain("completed");
    expect(byEpoch.get(1)!.events.map((e) => e.name)).toContain("lost_race");
    expect(byEpoch.get(1)!.status.code).not.toBe(SpanStatusCode.ERROR);
    const completes = named("complete detect", trace);
    expect(completes.map((s) => s.attributes["wildebeest.write.accepted"]).sort()).toEqual([false, true]);
    const lost = completes.find((s) => !s.attributes["wildebeest.write.accepted"])!;
    expect(lost.attributes["wildebeest.report.status"]).toBe("already_done");
    expect(lost.status.code).not.toBe(SpanStatusCode.ERROR); // losing a race is not an error
  });

  it("stamps synthetic benchmark tasks too", async () => {
    const { jobId } = await createSyntheticJob(5);
    const rows = await tasksOfJob(jobId);
    expect(rows.every((r) => TP.test(r.traceparent))).toBe(true);
    expect(named("create detect")).toHaveLength(5);
    expect(named("create job")).toHaveLength(1);
  });

  it("keeps an unsampled image unsampled end to end (head sampling) and exports nothing for it", async () => {
    sampleRoots = false;
    const { jobId } = await makeJob(["u1"]);
    const [row] = await tasksOfJob(jobId);
    expect(row.traceparent).toMatch(/-00$/); // stored, flagged not-sampled, so the worker agrees
    const w = await registerTestWorker("detect", "w1");
    const lease = (await pullAndClaim(w, "detect"))!;
    expect(lease.traceparent).toMatch(/-00$/);
    expect(traceOf(lease.traceparent!)).toBe(traceOf(row.traceparent));
    await completeTasks(w, [{ taskId: lease.taskId, leaseEpoch: 1, result: empty }]);
    expect(spans()).toHaveLength(0);
  });

  it("with tracing off: no spans, no traceparent on tasks or leases", async () => {
    setTracingEnabled(false);
    const { jobId } = await makeJob(["o1", "o2"]);
    await createSyntheticJob(3);
    expect((await tasksOfJob(jobId)).map((r) => r.traceparent)).toEqual([null, null]);
    const w1 = await registerTestWorker("detect", "w1");
    const w2 = await registerTestWorker("detect", "w2");
    const lease = (await pullAndClaim(w1, "detect"))!;
    expect(lease.traceparent).toBeNull();
    await recoverWorkers([w1], "docker_event");
    const again = (await pullAndClaim(w2, "detect"))!;
    await post(`/tasks/${again.taskId}/complete`, { workerId: w2, leaseEpoch: again.leaseEpoch, result: empty });
    await post(`/tasks/${lease.taskId}/fail`, { workerId: w1, leaseEpoch: 1, error: "late" });
    expect(spans()).toHaveLength(0);
  });
});

describe("GET /metrics/prom", () => {
  const scrape = async () => (await fetch(base + "/metrics/prom")).text();
  const value = (text: string, series: string) => {
    const line = text.split("\n").find((l) => l.startsWith(series + " "));
    return line ? Number(line.slice(series.length + 1)) : 0;
  };

  it("exports RED counters, histograms and gauges in Prometheus text format", async () => {
    const before = await scrape();
    const { jobId } = await makeJob(["m1", "m2"]);
    const w = await registerTestWorker("detect", "w1");
    const lease = (await pullAndClaim(w, "detect"))!;
    await post(`/tasks/${lease.taskId}/complete`, { workerId: w, leaseEpoch: lease.leaseEpoch, result: empty });
    await post(`/tasks/${lease.taskId}/complete`, { workerId: w, leaseEpoch: lease.leaseEpoch - 1, result: empty });
    await post(`/workers/${w}/heartbeat`, { taskIds: [], metrics: { tasksDone: 1, rssMb: 50, avgLatencyMs: 5, claimBatch: 2 } });
    expect(jobId).toBeTruthy();

    const text = await scrape();
    const delta = (s: string) => value(text, s) - value(before, s);
    expect(delta('wildebeest_tasks_completed_total{stage="detect"}')).toBe(1);
    expect(delta("wildebeest_stale_write_rejections_total")).toBe(1);
    expect(delta('wildebeest_task_service_seconds_count{stage="detect"}')).toBe(1);
    expect(delta('wildebeest_task_queue_wait_seconds_count{stage="detect"}')).toBe(1);
    expect(delta('wildebeest_http_request_duration_seconds_count{route="complete",code="200"}')).toBe(1);
    expect(delta('wildebeest_http_request_duration_seconds_count{route="complete",code="409"}')).toBe(1);
    expect(value(text, 'wildebeest_leases_in_flight{stage="detect"}')).toBe(0);
    expect(value(text, 'wildebeest_workers{stage="detect",status="ALIVE"}')).toBe(1);
    expect(value(text, `wildebeest_worker_claim_batch{worker="${w}",stage="detect"}`)).toBe(2);
    expect(text).toMatch(/^wildebeest_queue_depth\{stage="detect"\} \d+$/m);
    expect(text).toContain("wildebeest_coordinator_process_cpu_seconds_total");
  });
});
