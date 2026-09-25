// Phase 2 acceptance: with 2 detectors and 1 classifier, a 200-image sample job finishes
// and every image's category in the DB is consistent with its stage 1 detections.
import { afterAll, describe, expect, it } from "vitest";
import { api, db, startStack, waitForJobDone } from "./helpers";

const THRESHOLD = Number(process.env.ANIMAL_CONF_THRESHOLD ?? 0.2);
const pool = db();

afterAll(() => pool.end());

function expectedCategory(detections: { label: string; conf: number }[]) {
  const hit = (label: string) => detections.some((d) => d.label === label && d.conf >= THRESHOLD);
  if (hit("animal")) return "animal";
  if (hit("human")) return "human";
  if (hit("vehicle")) return "vehicle";
  return "empty";
}

describe("two-stage pipeline", () => {
  it("finishes a 200-image job with correct categories", async () => {
    await startStack(2, 1);
    await api("POST", "/admin/clear-cache");
    const res = await api("POST", "/jobs/sample", { size: 200, countryCode: "TZA" });
    expect(res.status).toBe(200);

    const job = await waitForJobDone(res.body.jobId);
    expect(job.processed).toBe(200);

    const { rows } = await pool.query(
      `select i.final_category, i.species_common_name, d.detections
         from images i join detection_results d on d.sha256 = i.sha256
        where i.job_id = $1`, [res.body.jobId]);
    expect(rows).toHaveLength(200);
    for (const row of rows) {
      expect(row.final_category).toBe(expectedCategory(row.detections));
      if (row.final_category === "animal") expect(row.species_common_name).toBeTruthy();
    }
  });
});
