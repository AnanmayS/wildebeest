import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { config } from "./config.js";
import { query, tx } from "./db.js";
import { isThrottled } from "./dispatcher.js";
import { hub, recordEvents, type EventInput } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { categorize, maybeFinishJob, type Category, type ClassificationRow, type Detection } from "./results.js";
import { imageKey, presign, putIfMissing } from "./storage.js";

// ---------------------------------------------------------------------------------------------
// Job creation
// ---------------------------------------------------------------------------------------------

export interface ImageInput {
  originalName: string;
  sha256: string;
  contentType?: string;
  /** Loads the bytes; only called if the object is not already in MinIO. */
  read: () => Promise<Buffer>;
}

export const sha256Of = (buf: Buffer) => createHash("sha256").update(buf).digest("hex");

/** Runs fn over items with at most `limit` in flight. */
async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]);
    }
  });
  await Promise.all(workers);
  return out;
}

interface PlannedImage {
  id: string;
  input: ImageInput;
  cacheHit: boolean;
  final: Category | null;
  species: ClassificationRow | null;
  task: "detect" | "classify" | null;
}

/**
 * Creates a job. For each image: content-hash cache lookup by (sha256, model_version).
 *  - detection cached and final without stage 2 (empty/human/vehicle), or animal with a cached
 *    classification → finalised right here, no tasks (cache_hit).
 *  - detection cached, animal, classification not cached → cache_hit, only a classify task.
 *  - otherwise → a detect task.
 * Tasks are created PENDING/unqueued; the dispatcher moves them into Redis. A fully cached job
 * is marked done inside the same transaction, so it is finished when this call returns.
 */
export async function createJob(opts: {
  name: string;
  countryCode?: string | null;
  sampleSize?: number | null;
  images: ImageInput[];
}): Promise<{ jobId: string; total: number; cacheHits: number; done: boolean }> {
  const shas = [...new Set(opts.images.map((i) => i.sha256))];
  const [dets, clss] = await Promise.all([
    query(`select sha256, detections from detection_results where model_version = $1 and sha256 = any($2::text[])`, [
      config.detectorModelVersion,
      shas,
    ]),
    query(
      `select sha256, label, common_name, confidence, crop_key from classification_results
        where model_version = $1 and sha256 = any($2::text[])`,
      [config.classifierModelVersion, shas],
    ),
  ]);
  const detBySha = new Map<string, Detection[]>(dets.rows.map((r) => [r.sha256, r.detections]));
  const clsBySha = new Map<string, ClassificationRow>(
    clss.rows.map((r) => [
      r.sha256,
      { label: r.label, commonName: r.common_name, confidence: r.confidence, cropKey: r.crop_key },
    ]),
  );

  const plan: PlannedImage[] = opts.images.map((input) => {
    const p: PlannedImage = { id: randomUUID(), input, cacheHit: false, final: null, species: null, task: "detect" };
    const det = detBySha.get(input.sha256);
    if (!det) return p;
    p.cacheHit = true;
    const category = categorize(det);
    if (category !== "animal") return { ...p, final: category, task: null };
    const cls = clsBySha.get(input.sha256);
    if (cls) return { ...p, final: "animal", species: cls, task: null };
    return { ...p, task: "classify" };
  });

  // Upload originals that a worker will need to download. Keys are content-addressed, so an
  // object that already exists is skipped. Fully cached images need no upload: their object was
  // stored when they were first processed.
  const toUpload = new Map<string, ImageInput>();
  for (const p of plan) if (p.task) toUpload.set(p.input.sha256, p.input);
  await mapLimit([...toUpload.values()], 16, (img) =>
    putIfMissing(imageKey(img.sha256), img.read, img.contentType ?? "image/jpeg"),
  );

  const jobId = randomUUID();
  const countryCode = (opts.countryCode || config.defaultCountry).toUpperCase();
  const cacheHits = plan.filter((p) => p.cacheHit).length;
  const withTask = plan.filter((p) => p.task);

  const { events, done } = await tx(async (c) => {
    await c.query(
      `insert into jobs (id, name, status, total_images, country_code, sample_size) values ($1, $2, 'running', $3, $4, $5)`,
      [jobId, opts.name, plan.length, countryCode, opts.sampleSize ?? null],
    );
    await c.query(
      `insert into images (id, job_id, sha256, object_key, original_name, final_category, species_label,
                           species_common_name, species_conf, cache_hit, finalized_at)
       select u.id, $1, u.sha, u.key, u.name, u.cat, u.label, u.common, u.conf, u.hit,
              case when u.cat is null then null else now() end
         from unnest($2::uuid[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[],
                     $9::real[], $10::boolean[])
           as u(id, sha, key, name, cat, label, common, conf, hit)`,
      [
        jobId,
        plan.map((p) => p.id),
        plan.map((p) => p.input.sha256),
        plan.map((p) => imageKey(p.input.sha256)),
        plan.map((p) => p.input.originalName),
        plan.map((p) => p.final),
        plan.map((p) => p.species?.label ?? null),
        plan.map((p) => p.species?.commonName ?? null),
        plan.map((p) => p.species?.confidence ?? null),
        plan.map((p) => p.cacheHit),
      ],
    );
    const taskIds = withTask.map(() => randomUUID());
    if (withTask.length > 0) {
      await c.query(
        // enqueued_at is spread by a microsecond per row so dispatch order follows input order.
        `insert into tasks (id, image_id, stage, state, enqueued_at)
         select u.id, u.image_id, u.stage, 'PENDING', now() + (u.ord * interval '1 microsecond')
           from unnest($1::uuid[], $2::uuid[], $3::text[]) with ordinality as u(id, image_id, stage, ord)`,
        [taskIds, withTask.map((p) => p.id), withTask.map((p) => p.task)],
      );
    }
    const ev: EventInput[] = withTask.map((p, i) => ({
      type: "enqueued",
      taskId: taskIds[i],
      detail: { stage: p.task, jobId },
    }));
    if (cacheHits > 0) ev.push({ type: "cache_hit", detail: { jobId, count: cacheHits, total: plan.length } });
    const finished = await maybeFinishJob(c, jobId);
    if (finished) ev.push(finished);
    return { events: await recordEvents(c, ev), done: Boolean(finished) };
  });

  hub.publishEvents(events);
  hub.jobChanged(jobId);
  console.log(
    `[jobs] created ${opts.name} (${jobId}): ${plan.length} images, ${cacheHits} cache hits, ${withTask.length} tasks`,
  );
  return { jobId, total: plan.length, cacheHits, done };
}

// ---------------------------------------------------------------------------------------------
// Sample dataset
// ---------------------------------------------------------------------------------------------

interface SampleRow {
  filename: string;
  label: string;
}

/** Minimal RFC 4180 CSV parser (quoted fields, doubled quotes). */
export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (ch === '"') quoted = false;
      else field += ch;
    } else if (ch === '"') quoted = true;
    else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      row.push(field);
      if (row.some((f) => f !== "")) rows.push(row);
      row = [];
      field = "";
    } else field += ch;
  }
  row.push(field);
  if (row.some((f) => f !== "")) rows.push(row);
  return rows;
}

/**
 * Deterministic, stratified ordering: within each label, files are sorted by name; then all rows
 * are ordered by their relative position inside their own label group. Any prefix of the result
 * therefore keeps roughly the dataset's label mix (e.g. ~70% empty), and a smaller sample is
 * always a prefix of a larger one, which keeps the content-hash cache useful across sizes.
 */
export function orderSample(rows: SampleRow[]): SampleRow[] {
  const groups = new Map<string, SampleRow[]>();
  for (const r of rows) {
    const g = groups.get(r.label) ?? [];
    g.push(r);
    groups.set(r.label, g);
  }
  const ranked: Array<{ row: SampleRow; pos: number }> = [];
  for (const g of groups.values()) {
    g.sort((a, b) => a.filename.localeCompare(b.filename));
    g.forEach((row, i) => ranked.push({ row, pos: (i + 0.5) / g.length }));
  }
  ranked.sort(
    (a, b) => a.pos - b.pos || a.row.label.localeCompare(b.row.label) || a.row.filename.localeCompare(b.row.filename),
  );
  return ranked.map((r) => r.row);
}

const IMAGE_EXT = /\.(jpe?g|png)$/i;

/** Sample files in selection order: labels.csv if present (only files that exist), else sorted names. */
export function listSample(dir = config.sampleDir): string[] {
  if (!fs.existsSync(dir)) return [];
  const present = new Set(fs.readdirSync(dir).filter((f) => IMAGE_EXT.test(f)));
  const labelsPath = path.join(dir, "labels.csv");
  if (fs.existsSync(labelsPath)) {
    const [header, ...body] = parseCsv(fs.readFileSync(labelsPath, "utf8"));
    const cols = header.map((h) => h.trim().toLowerCase());
    const fileCol = ["filename", "file_name", "file", "image", "path"].map((c) => cols.indexOf(c)).find((i) => i >= 0);
    const labelCol = ["label", "common_name", "species", "category"].map((c) => cols.indexOf(c)).find((i) => i >= 0);
    if (fileCol !== undefined) {
      const seen = new Set<string>();
      const rows: SampleRow[] = [];
      for (const r of body) {
        const filename = path.basename(r[fileCol] ?? "");
        if (!present.has(filename) || seen.has(filename)) continue;
        seen.add(filename);
        rows.push({ filename, label: labelCol !== undefined ? (r[labelCol] ?? "").trim().toLowerCase() : "" });
      }
      if (rows.length > 0) return orderSample(rows).map((r) => r.filename);
    }
  }
  return [...present].sort();
}

// sha256 per sample file, memoised on (size, mtime) so repeat sample jobs don't re-read the disk.
const shaMemo = new Map<string, { size: number; mtimeMs: number; sha: string }>();

async function hashFile(file: string): Promise<string> {
  const st = await fs.promises.stat(file);
  const memo = shaMemo.get(file);
  if (memo && memo.size === st.size && memo.mtimeMs === st.mtimeMs) return memo.sha;
  const sha = sha256Of(await fs.promises.readFile(file));
  shaMemo.set(file, { size: st.size, mtimeMs: st.mtimeMs, sha });
  return sha;
}

export class SampleUnavailableError extends Error {}

export async function createSampleJob(size: number, countryCode?: string | null) {
  const files = listSample();
  if (files.length === 0) throw new SampleUnavailableError(`no sample images found in ${config.sampleDir}`);
  const chosen = files.slice(0, Math.max(1, Math.min(Math.floor(size), files.length)));
  const images = await mapLimit(chosen, 32, async (name): Promise<ImageInput> => {
    const file = path.join(config.sampleDir, name);
    return {
      originalName: name,
      sha256: await hashFile(file),
      contentType: name.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg",
      read: () => fs.promises.readFile(file),
    };
  });
  return createJob({ name: `sample-${chosen.length}`, countryCode, sampleSize: chosen.length, images });
}

// ---------------------------------------------------------------------------------------------
// Read side
// ---------------------------------------------------------------------------------------------

export interface JobSummary {
  id: string;
  name: string;
  status: "running" | "done";
  createdAt: string;
  finishedAt: string | null;
  total: number;
  processed: number;
  failed: number;
  cacheHits: number;
  categories: Record<Category, number>;
  species: Array<{ commonName: string; count: number }>;
  elapsedMs: number;
  throughput: number;
  pending: { detect: number; classify: number };
  impact: { emptyPct: number; hoursSaved: number };
  countryCode: string | null;
  sampleSize: number | null;
  throttled: boolean;
  classifyQueue: number;
}

export async function jobSummary(jobId: string): Promise<JobSummary | null> {
  const { rows: jobs } = await query(`select * from jobs where id = $1`, [jobId]);
  if (jobs.length === 0) return null;
  const job = jobs[0];
  const [counts, species, pending, classifyQueue] = await Promise.all([
    query(
      `select count(final_category)::int as processed,
              count(*) filter (where cache_hit)::int as cache_hits,
              count(*) filter (where final_category = 'empty')::int as empty,
              count(*) filter (where final_category = 'animal')::int as animal,
              count(*) filter (where final_category = 'human')::int as human,
              count(*) filter (where final_category = 'vehicle')::int as vehicle,
              count(*) filter (where final_category = 'failed')::int as failed,
              count(*) filter (where finalized_at > now() - interval '5 seconds')::int as recent
         from images where job_id = $1`,
      [jobId],
    ),
    query(
      `select coalesce(species_common_name, 'unknown') as name, count(*)::int as n from images
        where job_id = $1 and final_category = 'animal' group by 1 order by 2 desc, 1`,
      [jobId],
    ),
    query(
      `select t.stage, count(*)::int as n from tasks t join images i on i.id = t.image_id
        where i.job_id = $1 and t.state in ('PENDING', 'LEASED') group by t.stage`,
      [jobId],
    ),
    getRedis().llen(keys.queue("classify")),
  ]);
  const c = counts.rows[0];
  const created = new Date(job.created_at);
  const finished = job.finished_at ? new Date(job.finished_at) : null;
  const pend = Object.fromEntries(pending.rows.map((r) => [r.stage, r.n]));
  const processed = c.processed;
  return {
    id: job.id,
    name: job.name,
    status: job.status,
    createdAt: created.toISOString(),
    finishedAt: finished?.toISOString() ?? null,
    total: job.total_images,
    processed,
    failed: c.failed,
    cacheHits: c.cache_hits,
    categories: { empty: c.empty, animal: c.animal, human: c.human, vehicle: c.vehicle, failed: c.failed },
    species: species.rows.map((r) => ({ commonName: r.name, count: r.n })),
    elapsedMs: (finished ?? new Date()).getTime() - created.getTime(),
    throughput: c.recent / 5,
    pending: { detect: pend.detect ?? 0, classify: pend.classify ?? 0 },
    impact: {
      emptyPct: processed > 0 ? Math.round((c.empty / processed) * 1000) / 10 : 0,
      hoursSaved: Math.round(((c.empty * config.humanReviewSecondsPerImage) / 3600) * 100) / 100,
    },
    countryCode: job.country_code,
    sampleSize: job.sample_size,
    throttled: isThrottled(),
    classifyQueue,
  };
}

export async function listJobs(limit = 20): Promise<JobSummary[]> {
  const { rows } = await query(`select id from jobs order by created_at desc limit $1`, [limit]);
  const out = await Promise.all(rows.map((r) => jobSummary(r.id)));
  return out.filter((j): j is JobSummary => j !== null);
}

export async function jobImages(
  jobId: string,
  opts: { category?: string; species?: string; page?: number; pageSize?: number },
) {
  const page = Math.max(1, Math.floor(opts.page ?? 1) || 1);
  const pageSize = Math.min(200, Math.max(1, Math.floor(opts.pageSize ?? 48) || 48));
  const where = ["i.job_id = $1"];
  const params: unknown[] = [jobId];
  if (opts.category) {
    params.push(opts.category);
    where.push(`i.final_category = $${params.length}`);
  }
  if (opts.species) {
    params.push(opts.species);
    where.push(`i.species_common_name = $${params.length}`);
  }
  const { rows: totalRows } = await query(`select count(*)::int as n from images i where ${where.join(" and ")}`, params);
  params.push(config.detectorModelVersion, config.classifierModelVersion, pageSize, (page - 1) * pageSize);
  const n = params.length;
  const { rows } = await query(
    `select i.*, d.detections, c.crop_key
       from images i
       left join detection_results d on d.sha256 = i.sha256 and d.model_version = $${n - 3}
       left join classification_results c on c.sha256 = i.sha256 and c.model_version = $${n - 2}
      where ${where.join(" and ")}
      order by i.finalized_at desc nulls last, i.created_at desc, i.id
      limit $${n - 1} offset $${n}`,
    params,
  );
  const images = await Promise.all(
    rows.map(async (r) => ({
      id: r.id,
      originalName: r.original_name,
      url: await presign(r.object_key),
      cropUrl: await presign(r.crop_key),
      category: r.final_category,
      commonName: r.species_common_name,
      speciesLabel: r.species_label,
      confidence: r.species_conf,
      detections: r.detections ?? [],
      cacheHit: r.cache_hit,
    })),
  );
  return { images, total: totalRows[0].n, page, pageSize };
}
