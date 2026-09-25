import { useCallback, useEffect, useRef, useState } from 'react';
import { api } from '../lib/api';
import type { GalleryImage, JobSummary } from '../lib/types';
import { fmtInt } from '../lib/format';
import { DetectionImage } from './DetectionImage';

const PAGE_SIZE = 24;
const AUTO_REFRESH_MS = 3000;

const CATEGORIES = [
  { value: 'animal', label: 'Animals' },
  { value: '', label: 'All' },
  { value: 'empty', label: 'Empty' },
  { value: 'human', label: 'People' },
  { value: 'vehicle', label: 'Vehicles' },
];

interface Props {
  job: JobSummary | null;
  /** Boxes below this confidence are not drawn (GET /config animalConfThreshold). */
  minBoxConf: number;
}

export function Gallery({ job, minBoxConf }: Props) {
  const [category, setCategory] = useState('animal');
  const [species, setSpecies] = useState('');
  const [page, setPage] = useState(1);
  const [data, setData] = useState<{ images: GalleryImage[]; total: number }>({ images: [], total: 0 });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const jobId = job?.id;

  const requestSeq = useRef(0);
  const load = useCallback(async () => {
    if (!jobId) return;
    const seq = ++requestSeq.current; // only the newest request may update the grid
    setLoading(true);
    try {
      const res = await api.listImages(jobId, { category, species, page, pageSize: PAGE_SIZE });
      if (seq === requestSeq.current) {
        setData(res);
        setError(null);
      }
    } catch (err) {
      if (seq === requestSeq.current) setError((err as Error).message);
    } finally {
      if (seq === requestSeq.current) setLoading(false);
    }
  }, [jobId, category, species, page]);

  // Reload immediately when the filters or page change.
  useEffect(() => {
    load();
  }, [load]);

  // New job: reset filters.
  useEffect(() => {
    setSpecies('');
    setPage(1);
    setData({ images: [], total: 0 });
  }, [jobId]);

  // While the job progresses, refresh at most every few seconds (trailing throttle).
  const latestLoad = useRef(load);
  latestLoad.current = load;
  const timer = useRef<number | null>(null);
  useEffect(() => {
    if (timer.current !== null) return;
    timer.current = window.setTimeout(() => {
      timer.current = null;
      latestLoad.current();
    }, AUTO_REFRESH_MS);
  }, [job?.processed]);
  useEffect(
    () => () => {
      clearTimeout(timer.current ?? undefined);
      timer.current = null;
    },
    [],
  );

  const pages = Math.max(1, Math.ceil(data.total / PAGE_SIZE));
  const from = data.total ? (page - 1) * PAGE_SIZE + 1 : 0;
  const to = Math.min(page * PAGE_SIZE, data.total);

  return (
    <section className="card px-5 pb-5 pt-4">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-baseline gap-3">
          <h2 className="text-lg font-semibold tracking-tight">Detections</h2>
          <span className="text-sm tabular text-ink-500">
            {data.total ? `${fmtInt(from)}–${fmtInt(to)} of ${fmtInt(data.total)}` : ''}
          </span>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="flex rounded-lg border border-ink-700 bg-ink-850 p-0.5" role="tablist">
            {CATEGORIES.map((c) => (
              <button
                key={c.value}
                role="tab"
                aria-selected={category === c.value}
                onClick={() => {
                  setCategory(c.value);
                  if (c.value !== 'animal' && c.value !== '') setSpecies('');
                  setPage(1);
                }}
                className={`h-8 rounded-md px-3 text-sm font-medium transition ${
                  category === c.value ? 'bg-ink-700 text-ink-100' : 'text-ink-400 hover:text-ink-100'
                }`}
              >
                {c.label}
              </button>
            ))}
          </div>

          <select
            value={species}
            onChange={(e) => {
              setSpecies(e.target.value);
              if (e.target.value) setCategory('animal');
              setPage(1);
            }}
            className="h-9 rounded-lg border border-ink-700 bg-ink-850 px-3 text-sm capitalize text-ink-100 outline-none"
            aria-label="Filter by species"
          >
            <option value="">All species</option>
            {job?.species.map((s) => (
              <option key={s.commonName} value={s.commonName}>
                {s.commonName} ({s.count})
              </option>
            ))}
          </select>

          <button className="btn btn-ghost" onClick={load} disabled={!jobId || loading}>
            {loading ? 'Loading…' : 'Refresh'}
          </button>
          <div className="flex items-center gap-1">
            <button className="btn btn-ghost px-2.5" disabled={page <= 1} onClick={() => setPage((p) => p - 1)} aria-label="Previous page">
              ‹
            </button>
            <span className="w-16 text-center text-sm tabular text-ink-400">
              {page} / {pages}
            </span>
            <button className="btn btn-ghost px-2.5" disabled={page >= pages} onClick={() => setPage((p) => p + 1)} aria-label="Next page">
              ›
            </button>
          </div>
        </div>
      </div>

      {error && <p className="mt-3 text-sm text-ember-400">Could not load images: {error}</p>}

      {data.images.length === 0 ? (
        <p className="mt-4 rounded-lg border border-dashed border-ink-800 px-4 py-10 text-center text-sm text-ink-500">
          {jobId ? 'Nothing here yet — images appear as they are finalised.' : 'Load a sample dataset to see detections.'}
        </p>
      ) : (
        <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6">
          {data.images.map((img) => (
            <GalleryCard key={img.id} image={img} minBoxConf={minBoxConf} />
          ))}
        </div>
      )}
    </section>
  );
}

function GalleryCard({ image, minBoxConf }: { image: GalleryImage; minBoxConf: number }) {
  const title = image.commonName ?? CATEGORY_LABEL[image.category ?? ''] ?? 'Pending';
  return (
    <figure className="animate-rise overflow-hidden rounded-lg border border-ink-800 bg-ink-850">
      <div className="relative">
        <DetectionImage image={image} minBoxConf={minBoxConf} />
        {image.cacheHit && (
          <span className="absolute bottom-7 right-1.5 rounded bg-ink-950/80 px-1.5 py-px text-[10px] font-semibold uppercase tracking-wider text-sky-400">
            cache hit
          </span>
        )}
      </div>
      <figcaption className="flex items-center justify-between gap-2 px-2.5 py-2">
        <span className="truncate text-sm font-medium capitalize">{title}</span>
        {image.confidence != null && <span className="shrink-0 text-sm tabular text-leaf-400">{Math.round(image.confidence * 100)}%</span>}
      </figcaption>
    </figure>
  );
}

const CATEGORY_LABEL: Record<string, string> = {
  empty: 'Empty', animal: 'Animal', human: 'Person', vehicle: 'Vehicle', failed: 'Failed',
};
