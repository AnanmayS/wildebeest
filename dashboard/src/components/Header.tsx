import { useRef, useState } from 'react';
import { uploadPhotos } from '../lib/api';
import type { Connection } from '../hooks/useWildebeest';
import { Logo } from './Logo';

const SIZES = [500, 1000, 2000];

interface Props {
  connection: Connection;
  lastSampleSize: number | null;
  onStartSample: (size: number) => Promise<void>;
  onUploaded: (jobId: string) => Promise<void>;
  onError: (message: string) => void;
}

export function Header({ connection, lastSampleSize, onStartSample, onUploaded, onError }: Props) {
  const [size, setSize] = useState(1000);
  const [starting, setStarting] = useState(false);
  const [uploadPct, setUploadPct] = useState<number | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);

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
    <header className="flex items-center justify-between gap-6">
      <div className="flex min-w-0 items-center gap-4">
        <Logo />
        <div className="min-w-0">
          <div className="flex items-baseline gap-3">
            <h1 className="text-[26px] font-semibold leading-none tracking-tight">Wildebeest</h1>
            <ConnectionPill connection={connection} />
          </div>
          <p className="mt-1.5 max-w-[600px] text-[13px] leading-snug text-ink-400">
            {PITCH}
          </p>
        </div>
      </div>

      <div className="flex shrink-0 items-center gap-2">
        <input ref={fileInput} type="file" accept="image/jpeg,image/png" multiple hidden onChange={(e) => upload(e.target.files)} />
        <button className="btn btn-ghost" disabled={uploadPct !== null} onClick={() => fileInput.current?.click()}>
          <UploadIcon />
          {uploadPct === null ? 'Upload photos' : `Uploading ${Math.round(uploadPct * 100)}%`}
        </button>
        <button
          className="btn btn-ghost"
          disabled={!lastSampleSize || starting}
          onClick={() => lastSampleSize && run(lastSampleSize)}
          title="Re-submit the same photos: every image is a content-hash cache hit"
        >
          <RerunIcon />
          Rerun same batch
        </button>
        <div className="ml-2 flex items-center rounded-lg border border-ink-700 bg-ink-850 p-0.5">
          <label className="sr-only" htmlFor="sample-size">Sample size</label>
          <select
            id="sample-size"
            value={size}
            onChange={(e) => setSize(Number(e.target.value))}
            className="h-8 cursor-pointer appearance-none rounded-md bg-transparent pl-3 pr-2 text-sm font-medium tabular text-ink-100 outline-none"
          >
            {SIZES.map((n) => (
              <option key={n} value={n} className="bg-ink-900">
                {n.toLocaleString()} photos
              </option>
            ))}
          </select>
          <button className="btn btn-primary h-8" disabled={starting} onClick={() => run(size)}>
            {starting ? 'Starting…' : 'Load sample dataset'}
          </button>
        </div>
      </div>
    </header>
  );
}

const PITCH =
  'Upload 1,000 trail-cam photos, and a cluster of workers sorts them into “empty” and “41 zebras, 12 lions, 8 elephants” — even if a machine dies halfway through.';

function ConnectionPill({ connection }: { connection: Connection }) {
  const live = connection === 'live';
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] ${
        live ? 'bg-leaf-400/10 text-leaf-400' : 'bg-sun-400/10 text-sun-400'
      }`}
      title={live ? 'WebSocket connected' : 'WebSocket down — polling every 2s'}
    >
      <span className={`h-1.5 w-1.5 rounded-full ${live ? 'bg-leaf-400 animate-pulse-dot' : 'bg-sun-400'}`} />
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
