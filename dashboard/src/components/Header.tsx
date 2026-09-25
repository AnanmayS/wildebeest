import { useRef, useState } from 'react';
import { uploadPhotos } from '../lib/api';
import type { Connection, LiveJob } from '../hooks/useWildebeest';
import type { Chaos } from '../lib/types';
import { fmtDuration, fmtInt } from '../lib/format';
import { useNow } from '../hooks/useNow';
import { ChaosControl } from './ChaosControl';
import { Logo } from './Logo';

const SIZES = [300, 1000, 2000];

interface Props {
  connection: Connection;
  job: LiveJob | null;
  chaos: Chaos;
  onChaos: (next: Chaos) => void;
  onStartSample: (size: number) => Promise<void>;
  onUploaded: (jobId: string) => Promise<void>;
  onError: (message: string) => void;
  /** Grafana (traces + Prometheus), when the observability profile is part of this deployment. */
  grafanaUrl?: string | null;
}

/** Title, the active job at a glance, and every demo control that isn't on a worker lane. */
export function Header({ connection, job, chaos, onChaos, onStartSample, onUploaded, onError, grafanaUrl }: Props) {
  const [size, setSize] = useState(1000);
  const [starting, setStarting] = useState(false);
  const [uploadPct, setUploadPct] = useState<number | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  // Prefer the explicit field; older coordinators only encode it in the name ("sample-1000").
  const lastSampleSize = job ? (job.sampleSize ?? (Number(job.name.match(/^sample-(\d+)$/)?.[1]) || null)) : null;

  const run = async (n: number) => {
    setStarting(true);
    try {
      await onStartSample(n);
    } catch (err) {
      onError(`Could not start sample job: ${(err as Error).message}`);
    } finally {
      setStarting(false);
    }
  };

  const upload = async (files: FileList | null) => {
    if (!files?.length) return;
    setUploadPct(0);
    try {
      const { jobId } = await uploadPhotos([...files], setUploadPct);
      await onUploaded(jobId);
    } catch (err) {
      onError((err as Error).message);
    } finally {
      setUploadPct(null);
      if (fileInput.current) fileInput.current.value = '';
    }
  };

  return (
    <header className="flex h-[52px] items-center gap-5">
      <div className="flex shrink-0 items-center gap-3">
        <Logo />
        <div>
          <div className="flex items-center gap-2.5">
            <h1 className="text-[22px] font-semibold leading-none tracking-tight">Wildebeest</h1>
            <ConnectionPill connection={connection} />
          </div>
          <p className="mt-1 text-[12.5px] leading-none text-ink-400">
            Fault-tolerant distributed camera-trap pipeline
            {grafanaUrl && (
              <>
                {' · '}
                <a href={grafanaUrl} target="_blank" rel="noreferrer" className="text-ink-300 underline decoration-ink-600 underline-offset-2 hover:text-leaf-300" title="Per-image traces (Tempo) and Prometheus metrics">
                  Open traces in Grafana ↗
                </a>
              </>
            )}
          </p>
        </div>
      </div>

      <JobStrip job={job} />

      <div className="flex shrink-0 items-center gap-2">
        <ChaosControl chaos={chaos} onChange={onChaos} />
        <input ref={fileInput} type="file" accept="image/jpeg,image/png" multiple hidden onChange={(e) => upload(e.target.files)} />
        <button className="btn btn-ghost px-2.5" disabled={uploadPct !== null} onClick={() => fileInput.current?.click()} title="Upload your own photos">
          <UploadIcon />
          {uploadPct === null ? <span className="sr-only">Upload photos</span> : `${Math.round(uploadPct * 100)}%`}
        </button>
        <button
          className="btn btn-ghost"
          disabled={!lastSampleSize || starting}
          onClick={() => lastSampleSize && run(lastSampleSize)}
          title="Submit the same photos again: every one is a content-hash cache hit and finalises without inference"
        >
          <RerunIcon />
          Rerun (cache)
        </button>
        <div className="flex items-center rounded-lg border border-ink-700 bg-ink-850 p-0.5">
          <label className="sr-only" htmlFor="sample-size">Sample size</label>
          <select
            id="sample-size"
            value={size}
            onChange={(e) => setSize(Number(e.target.value))}
            className="h-8 cursor-pointer appearance-none rounded-md bg-transparent pl-3 pr-2 text-sm font-medium tabular text-ink-100 outline-none"
          >
            {SIZES.map((n) => (
              <option key={n} value={n} className="bg-ink-900">{n.toLocaleString()} photos</option>
            ))}
          </select>
          <button className="btn btn-primary h-8" disabled={starting} onClick={() => run(size)}>
            {starting ? 'Starting…' : 'Load sample'}
          </button>
        </div>
      </div>
    </header>
  );
}

/** The active job in one line: name, progress bar coloured by outcome, count and clock. */
function JobStrip({ job }: { job: LiveJob | null }) {
  const now = useNow(500);
  if (!job) return <div className="min-w-0 flex-1 text-[13px] text-ink-500">No job yet: load a sample to watch the pipeline work.</div>;
  const running = job.status === 'running';
  const elapsed = job.elapsedMs + (running ? now - job.receivedAt : 0);
  const c = job.categories;
  const total = job.total || 1;
  const segs = [
    { n: c.empty, cls: 'bg-dust-400/80', label: 'empty' },
    { n: c.animal, cls: 'bg-leaf-400', label: 'animal' },
    { n: c.human + c.vehicle, cls: 'bg-sky-400', label: 'people / vehicles' },
    { n: c.failed, cls: 'bg-ember-400', label: 'failed' },
  ];
  return (
    <a href="#results" className="group min-w-0 flex-1" title="Jump to the job's results">
      <div className="flex items-baseline gap-2 text-[13px]">
        <span className="truncate font-mono text-ink-300">{job.name}</span>
        <span className={running ? 'text-leaf-400' : 'text-ink-400'}>{running ? 'running' : job.status}</span>
        <span className="ml-auto shrink-0 tabular text-ink-300">
          <b className="font-semibold text-ink-100">{fmtInt(job.processed)}</b> / {fmtInt(job.total)} · {fmtDuration(elapsed)}
        </span>
      </div>
      <div className="mt-1.5 flex h-1.5 gap-[2px] overflow-hidden rounded-full bg-ink-800">
        {segs.filter((s) => s.n > 0).map((s) => (
          <span key={s.label} title={`${fmtInt(s.n)} ${s.label}`} className={`h-full ${s.cls} transition-[width] duration-700`} style={{ width: `${(s.n / total) * 100}%` }} />
        ))}
      </div>
    </a>
  );
}

function ConnectionPill({ connection }: { connection: Connection }) {
  const live = connection === 'live';
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] ${
        live ? 'bg-leaf-400/10 text-leaf-400' : 'bg-sun-400/10 text-sun-400'
      }`}
      title={live ? 'WebSocket connected' : 'WebSocket down: polling every 2 s'}
    >
      <span className={`h-1.5 w-1.5 rounded-full ${live ? 'bg-leaf-400' : 'bg-sun-400'}`} />
      {live ? 'Live' : 'Polling'}
    </span>
  );
}

const UploadIcon = () => (
  <svg viewBox="0 0 16 16" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden>
    <path d="M8 11V2.5M4.5 6 8 2.5 11.5 6M2.5 11v2.5h11V11" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);
const RerunIcon = () => (
  <svg viewBox="0 0 16 16" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden>
    <path d="M13 8a5 5 0 1 1-1.6-3.7M13 2.5v3h-3" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);
