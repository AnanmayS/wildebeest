export const fmtInt = (n: number) => Math.round(n).toLocaleString('en-US');

/** "4.2s" under a minute (so a cached rerun reads as "0.3s", not "0:00"), "m:ss" above. */
export function fmtDuration(ms: number) {
  if (ms < 60_000) return `${(Math.max(0, ms) / 1000).toFixed(1)}s`;
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  return `${m}:${String(s % 60).padStart(2, '0')}`;
}

export const fmtPct = (n: number) => `${n.toFixed(n >= 10 || n === 0 ? 0 : 1)}%`;

/** "~35 minutes" below an hour, "~2.4 hours" above — both read better than "~0.6 hours". */
export function fmtReviewTime(hours: number) {
  if (hours < 1) return `~${Math.max(1, Math.round(hours * 60))} minutes`;
  return `~${hours.toFixed(1)} hours`;
}

export const shortId = (id: string) => id.replace(/^(detect|classify)-/, '');

export function timeOfDay(iso: string) {
  return new Date(iso).toLocaleTimeString('en-GB', { hour12: false });
}

/** "8 ms", "184 ms", "1.4 s", "12 s": milliseconds at the precision a reader needs. */
export function fmtMs(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return '—';
  if (ms === 0) return '0 ms';
  if (ms < 10) return `${ms.toFixed(ms < 1 ? 1 : 0)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms / 1000)} s`;
}

/** "just now", "12 s ago", "4 min ago". */
export function fmtAgo(iso: string | number, now: number): string {
  const s = Math.max(0, (now - (typeof iso === 'number' ? iso : Date.parse(iso))) / 1000);
  if (s < 2) return 'just now';
  if (s < 60) return `${Math.floor(s)} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return `${Math.floor(s / 3600)} h ago`;
}

export const fmtRate = (n: number) => (n >= 100 ? fmtInt(n) : n >= 10 ? n.toFixed(0) : n.toFixed(1));

/** First 6 hex chars of a task uuid: enough to follow one task across panels. */
export const shortTask = (id: string) => id.replace(/-/g, '').slice(0, 6);

/** Whole seconds for configured durations: "6 s", "15 s", "2.5 s". */
export const fmtSec = (ms: number) => `${Math.round(ms / 100) / 10} s`;
