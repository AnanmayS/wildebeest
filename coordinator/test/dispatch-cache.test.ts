import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { config } from "../src/config.js";
import { getPool, query } from "../src/db.js";
import { dispatchOnce, isThrottled } from "../src/dispatcher.js";
import { jobSummary, listSample, orderSample, parseCsv } from "../src/jobs.js";
import { getRedis, keys } from "../src/redis.js";
import { categorize, enqueueClassify, storeDetection } from "../src/results.js";
import { completeTask } from "../src/tasks.js";
import {
  classifyResult,
  detectResult,
  events,
  fakeImage,
  imagesOfJob,
  job,
  makeJob,
  pullAndClaim,
  registerTestWorker,
  resetState,
  tasksOfJob,
  teardown,
} from "./helpers.js";

beforeEach(resetState);
afterAll(teardown);

describe("dispatcher", () => {
  it("tops queue:detect up to DETECT_QUEUE_TARGET, oldest first, and never above it", async () => {
    const { jobId } = await makeJob(Array.from({ length: 8 }, (_, i) => `img${i}`));
    const tasks = await tasksOfJob(jobId);
    expect(tasks.every((t) => t.state === "PENDING" && !t.queued)).toBe(true);

    expect((await dispatchOnce()).detect).toBe(config.detectQueueTarget);
    const queued = await getRedis().lrange(keys.queue("detect"), 0, -1);
    expect(queued).toEqual(tasks.slice(0, config.detectQueueTarget).map((t) => t.id));

    expect((await dispatchOnce()).detect).toBe(0);
    await getRedis().lpop(keys.queue("detect"), 2);
    expect((await dispatchOnce()).detect).toBe(2);
    expect(await getRedis().llen(keys.queue("detect"))).toBe(config.detectQueueTarget);
  });

  it("applies backpressure with hysteresis on queue:classify", async () => {
    const redis = getRedis();
    await makeJob(["a", "b", "c"]);
    const high = config.classifyQueueHighWater; // 5 in tests
    const low = config.classifyQueueLowWater; // 2 in tests
    const fill = async (n: number) => {
      await redis.del(keys.queue("classify"));
      if (n > 0) await redis.rpush(keys.queue("classify"), ...Array.from({ length: n }, (_, i) => `dummy-${i}`));
    };

    await fill(high); // at the mark, not above: no throttle
    expect((await dispatchOnce()).throttled).toBe(false);
    await redis.del(keys.queue("detect"));
    await query(`update tasks set queued = false`);

    await fill(high + 1);
    const r = await dispatchOnce();
    expect(r).toMatchObject({ throttled: true, detect: 0 });
    expect(await redis.get(keys.throttled)).toBe("1");
    expect(await redis.llen(keys.queue("detect"))).toBe(0);

    await fill(low); // between the marks: stays throttled
    expect((await dispatchOnce()).throttled).toBe(true);

    await fill(low - 1); // below low water: released, detect flows again
    const released = await dispatchOnce();
    expect(released.throttled).toBe(false);
    expect(released.detect).toBe(3);
    expect(isThrottled()).toBe(false);
    expect(await redis.get(keys.throttled)).toBeNull();

    expect(await events("throttled")).toHaveLength(1);
    expect(await events("unthrottled")).toHaveLength(1);
    expect((await events("throttled"))[0].task_id).toBeNull();
  });
});

describe("idempotency", () => {
  it("keeps the first result for a (sha256, model_version) and categorises every copy from it", async () => {
    const a = await makeJob(["same"], "first");
    const b = await makeJob(["same"], "second"); // same photo, submitted before any result existed
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const l1 = (await pullAndClaim(w, "detect"))!;
    const l2 = (await pullAndClaim(w, "detect"))!;
    await completeTask(l1.taskId, w, l1.leaseEpoch, detectResult([{ label: "human", conf: 0.9 }]));
    await completeTask(l2.taskId, w, l2.leaseEpoch, detectResult([{ label: "vehicle", conf: 0.9 }]));

    const { rows } = await query(`select detections from detection_results`);
    expect(rows).toHaveLength(1);
    expect(rows[0].detections[0].label).toBe("human");
    expect((await imagesOfJob(a.jobId))[0].final_category).toBe("human");
    expect((await imagesOfJob(b.jobId))[0].final_category).toBe("human");
  });

  it("storeDetection is a no-op the second time", async () => {
    const sha = fakeImage("x").sha256;
    const first = await storeDetection(getPool(), sha, [{ label: "animal", conf: 0.9, bbox: [0, 0, 1, 1] }]);
    const second = await storeDetection(getPool(), sha, []);
    expect(second).toEqual(first);
  });

  it("enqueues the classify task at most once per image", async () => {
    const { jobId } = await makeJob(["z"]);
    const [img] = await imagesOfJob(jobId);
    const first = await enqueueClassify(getPool(), img.id);
    const second = await enqueueClassify(getPool(), img.id);
    expect(first).not.toBeNull();
    expect(second).toBeNull();
    const { rows } = await query(`select count(*)::int as n from tasks where image_id = $1 and stage = 'classify'`, [
      img.id,
    ]);
    expect(rows[0].n).toBe(1);
  });
});

describe("content-hash cache", () => {
  async function processAll(jobId: string, detections: Record<string, any>, species: Record<string, string>) {
    const det = await registerTestWorker("detect", "d1");
    const cls = await registerTestWorker("classify", "c1");
    for (;;) {
      await dispatchOnce();
      const lease = (await pullAndClaim(det, "detect")) ?? (await pullAndClaim(cls, "classify"));
      if (!lease) break;
      const { rows } = await query(`select original_name from images where sha256 = $1 limit 1`, [lease.sha256]);
      const name = rows[0].original_name.replace(".jpg", "");
      const result = lease.stage === "detect" ? detections[name] : classifyResult(species[name]);
      expect(await completeTask(lease.taskId, lease.stage === "detect" ? det : cls, lease.leaseEpoch, result)).toEqual(
        { status: "ok" },
      );
    }
    expect(await job(jobId)).toMatchObject({ status: "done" });
  }

  it("finishes a resubmitted job immediately, with no tasks, from cached results", async () => {
    const seeds = ["empty1", "person", "lion"];
    const first = await makeJob(seeds);
    await processAll(
      first.jobId,
      {
        empty1: detectResult([]),
        person: detectResult([{ label: "human", conf: 0.8 }]),
        lion: detectResult([{ label: "animal", conf: 0.9 }]),
      },
      { lion: "lion" },
    );

    const again = await makeJob(seeds);
    expect(again).toMatchObject({ total: 3, cacheHits: 3, done: true });
    expect(await tasksOfJob(again.jobId)).toEqual([]);
    const imgs = await imagesOfJob(again.jobId);
    expect(imgs.map((i) => [i.original_name, i.final_category, i.species_common_name, i.cache_hit])).toEqual([
      ["empty1.jpg", "empty", null, true],
      ["lion.jpg", "animal", "lion", true],
      ["person.jpg", "human", null, true],
    ]);
    const summary = (await jobSummary(again.jobId))!;
    expect(summary).toMatchObject({ status: "done", total: 3, processed: 3, cacheHits: 3 });
    expect(summary.species).toEqual([{ commonName: "lion", count: 1 }]);
    const [hit] = await events("cache_hit");
    expect(hit.detail).toMatchObject({ jobId: again.jobId, count: 3 });
  });

  it("skips only stage 1 when the detection is cached but the classification is not", async () => {
    const sha = fakeImage("giraffe").sha256;
    await storeDetection(getPool(), sha, [{ label: "animal", conf: 0.7, bbox: [0, 0, 1, 1] }]);
    const { jobId, cacheHits, done } = await makeJob(["giraffe"]);
    expect({ cacheHits, done }).toEqual({ cacheHits: 1, done: false });
    const tasks = await tasksOfJob(jobId);
    expect(tasks.map((t) => t.stage)).toEqual(["classify"]);
  });

  it("changing the model version invalidates the cache", async () => {
    await storeDetection(getPool(), fakeImage("m").sha256, []);
    const old = config.detectorModelVersion;
    config.detectorModelVersion = "test-detector-v2";
    try {
      const { cacheHits } = await makeJob(["m"]);
      expect(cacheHits).toBe(0);
    } finally {
      config.detectorModelVersion = old;
    }
  });
});

describe("categorisation and sample selection", () => {
  it("categorises by threshold with animal > human > vehicle precedence", () => {
    const t = 0.2;
    expect(categorize([], t)).toBe("empty");
    expect(categorize([{ label: "animal", conf: 0.19, bbox: [] }], t)).toBe("empty");
    expect(categorize([{ label: "vehicle", conf: 0.3, bbox: [] }, { label: "human", conf: 0.2, bbox: [] }], t)).toBe(
      "human",
    );
    expect(categorize([{ label: "human", conf: 0.99, bbox: [] }, { label: "animal", conf: 0.2, bbox: [] }], t)).toBe(
      "animal",
    );
  });

  it("parses quoted CSV", () => {
    expect(parseCsv('a,b\n"x,1","say ""hi"""\r\n')).toEqual([
      ["a", "b"],
      ["x,1", 'say "hi"'],
    ]);
  });

  it("orders the sample so any prefix keeps the label mix, deterministically", () => {
    const rows = [
      ...Array.from({ length: 70 }, (_, i) => ({ filename: `e${String(i).padStart(3, "0")}.jpg`, label: "empty" })),
      ...Array.from({ length: 20 }, (_, i) => ({ filename: `z${String(i).padStart(3, "0")}.jpg`, label: "zebra" })),
      ...Array.from({ length: 10 }, (_, i) => ({ filename: `l${String(i).padStart(3, "0")}.jpg`, label: "lion" })),
    ];
    const shuffled = [...rows].sort(() => Math.random() - 0.5);
    const ordered = orderSample(shuffled);
    expect(ordered).toEqual(orderSample(rows));
    const first10 = ordered.slice(0, 10);
    expect(first10.filter((r) => r.label === "empty")).toHaveLength(7);
    expect(first10.filter((r) => r.label === "zebra")).toHaveLength(2);
    expect(first10.filter((r) => r.label === "lion")).toHaveLength(1);
  });

  it("lists sample files from labels.csv (existing files only), else sorted filenames", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fg-sample-"));
    for (const f of ["b.jpg", "a.jpg", "c.jpg"]) fs.writeFileSync(path.join(dir, f), f);
    expect(listSample(dir)).toEqual(["a.jpg", "b.jpg", "c.jpg"]);
    fs.writeFileSync(
      path.join(dir, "labels.csv"),
      "filename,label,common_name\nc.jpg,zebra,zebra\na.jpg,empty,\nmissing.jpg,lion,lion\nb.jpg,empty,\n",
    );
    expect(listSample(dir)).toEqual(["a.jpg", "c.jpg", "b.jpg"]);
    fs.rmSync(dir, { recursive: true });
  });
});
