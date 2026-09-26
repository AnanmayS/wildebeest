import { useEffect, useRef, useState } from 'react';
import { api } from '../../lib/api';
import type { GalleryImage, JobSummary } from '../../lib/types';
import { fmtInt } from '../../lib/format';

const COUNT = 12;
const REFRESH_MS = 3000;

/** The human part: a row of the animals the last job found, cropped to the animal, with its name. */
export function AnimalStrip({ job }: { job: JobSummary | null }) {
  const [images, setImages] = useState<GalleryImage[]>([]);
  const [total, setTotal] = useState(0);
  const jobId = job?.id;
  const lastLoad = useRef(0);
  const currentJob = useRef(jobId);
  currentJob.current = jobId;

  // New job: start empty (declared first so the load below sees the reset).
  useEffect(() => {
    setImages([]);
    setTotal(0);
    lastLoad.current = 0;
  }, [jobId]);

  // Reload on a new job at once; while it runs, at most every few seconds.
  useEffect(() => {
    if (!jobId) return;
    const load = () => {
      lastLoad.current = Date.now();
      api.listImages(jobId, { category: 'animal', page: 1, pageSize: COUNT })
        .then((r) => {
          if (currentJob.current !== jobId) return; // answer for a job we've moved on from
          setImages(r.images);
          setTotal(r.total);
        })
        .catch(() => {});
    };
    const wait = Math.max(0, lastLoad.current + REFRESH_MS - Date.now());
    const t = window.setTimeout(load, wait);
    return () => clearTimeout(t);
  }, [jobId, job?.categories.animal]);

  return (
    <section className="card px-5 pb-3.5 pt-3">
      <div className="flex items-baseline gap-3">
        <h2 className="text-[17px] font-semibold tracking-tight">Animals it found</h2>
        {total > 0 && <span className="text-[13px] text-ink-500">{fmtInt(total)} in the last run · <a href="#engineer" className="underline decoration-ink-600 underline-offset-2 hover:text-ink-300">see them all</a></span>}
      </div>
      {images.length === 0 ? (
        <p className="mt-2 text-[14px] text-ink-500">Animals appear here as the photos are sorted.</p>
      ) : (
        <div className="mt-2 grid grid-cols-6 lg:grid-cols-12 gap-2.5">
          {images.map((img) => (
            <figure key={img.id} className="animate-rise overflow-hidden rounded-lg border border-ink-800 bg-ink-850">
              <img src={img.cropUrl ?? img.url} alt={img.commonName ?? 'animal'} loading="lazy" className="block aspect-[3/2] w-full object-cover" />
              <figcaption className="truncate px-2 py-0.5 text-[13px] font-medium capitalize text-ink-300">{img.commonName ?? 'Animal'}</figcaption>
            </figure>
          ))}
        </div>
      )}
    </section>
  );
}
