import { randomUUID } from "node:crypto";
import { config } from "./config.js";
import type { Db } from "./db.js";
import { finishLockThreshold } from "./dispatcher.js";
import type { EventInput } from "./events.js";

// Idempotent result storage and image/job finalisation.
//
// Results are keyed by (sha256, model_version) and written with ON CONFLICT DO NOTHING, so a
// retried task (or the same photo in another job) can never produce a second row. Callers always
// read the stored row back and categorise from *that*, so the category in `images` always
// matches the one result that was kept.

export type Category = "empty" | "animal" | "human" | "vehicle" | "failed";

export interface Detection {
  label: "animal" | "human" | "vehicle" | string;
  conf: number;
  bbox: number[];
}

export interface ClassificationRow {
  label: string | null;
  commonName: string | null;
  confidence: number | null;
  cropKey: string | null;
}

/** CONTRACTS.md "Final categorisation after stage 1". */
export function categorize(detections: Detection[], threshold = config.animalConfThreshold): Exclude<Category, "failed"> {
  const hit = (label: string) => detections.some((d) => d.label === label && Number(d.conf) >= threshold);
  if (hit("animal")) return "animal";
  if (hit("human")) return "human";
  if (hit("vehicle")) return "vehicle";
  return "empty";
}

/**
 * Stage 2 can overrule stage 1: when the species classifier looks at the animal crop and says
 * "blank", the detector's box was a false positive (grass, shadows), so the photo is empty.
 * In the baseline this lifted empty-vs-animal accuracy from 92.1% to 95.2% without losing an animal.
 */
export function categorizeSpecies(species: ClassificationRow | null | undefined): "animal" | "empty" {
  return species?.commonName?.toLowerCase() === "blank" ? "empty" : "animal";
}

/** Inserts the detection result unless one exists; returns whichever row is stored. */
export async function storeDetection(db: Db, sha256: string, detections: Detection[]): Promise<Detection[]> {
  await db.query(
    `insert into detection_results (sha256, model_version, detections) values ($1, $2, $3)
     on conflict (sha256, model_version) do nothing`,
    [sha256, config.detectorModelVersion, JSON.stringify(detections)],
  );
  const { rows } = await db.query(
    `select detections from detection_results where sha256 = $1 and model_version = $2`,
    [sha256, config.detectorModelVersion],
  );
  return rows[0].detections;
}

export async function storeClassification(
  db: Db,
  sha256: string,
  r: { label: string | null; commonName: string | null; confidence: number | null; cropKey: string | null; raw: unknown },
): Promise<ClassificationRow> {
  await db.query(
    `insert into classification_results (sha256, model_version, label, common_name, confidence, crop_key, raw)
     values ($1, $2, $3, $4, $5, $6, $7)
     on conflict (sha256, model_version) do nothing`,
    [sha256, config.classifierModelVersion, r.label, r.commonName, r.confidence, r.cropKey, JSON.stringify(r.raw ?? null)],
  );
  const found = await getClassification(db, sha256);
  return found!;
}

export async function getClassification(db: Db, sha256: string): Promise<ClassificationRow | null> {
  const { rows } = await db.query(
    `select label, common_name, confidence, crop_key from classification_results
      where sha256 = $1 and model_version = $2`,
    [sha256, config.classifierModelVersion],
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return { label: r.label, commonName: r.common_name, confidence: r.confidence, cropKey: r.crop_key };
}

/**
 * Creates the stage 2 task for an image. The unique (image_id, stage) constraint makes this a
 * no-op when a retried stage 1 completion tries to enqueue it again. Returns the new task ID, or
 * null if it already existed.
 */
export async function enqueueClassify(db: Db, imageId: string): Promise<string | null> {
  const { rows } = await db.query(
    `insert into tasks (id, image_id, stage, state) values ($1, $2, 'classify', 'PENDING')
     on conflict (image_id, stage) do nothing
     returning id`,
    [randomUUID(), imageId],
  );
  return rows[0]?.id ?? null;
}

/**
 * Sets the image's final category once (wb_finalize_image, migration 006, applies the "blank"
 * rule too). Returns the job ID if this call finalised it.
 */
export async function finalizeImage(
  db: Db,
  imageId: string,
  category: Category,
  species?: ClassificationRow | null,
): Promise<string | null> {
  const { rows } = await db.query(`select wb_finalize_image($1, $2, $3, $4, $5) as job_id`, [
    imageId,
    category,
    species?.label ?? null,
    species?.commonName ?? null,
    species?.confidence ?? null,
  ]);
  return rows[0]?.job_id ?? null;
}

/**
 * Marks the job done if every image has a final category (wb_finish_job, migration 006). Must run
 * inside the transaction that finalised the image. The job row is only locked near the end of a
 * job (at most `lockThreshold` unfinalised images left), which serialises two "last" images
 * finishing at once; earlier finalisations skip the lock entirely, and the reaper's sweep
 * (finishCompletedJobs) covers a race the threshold misses.
 */
export async function maybeFinishJob(
  db: Db,
  jobId: string,
  lockThreshold = finishLockThreshold(),
): Promise<EventInput | null> {
  const { rows } = await db.query(`select name, total from wb_finish_job($1, $2)`, [jobId, lockThreshold]);
  if (rows.length === 0) return null;
  return { type: "job_done", detail: { jobId, name: rows[0].name, total: rows[0].total } };
}
