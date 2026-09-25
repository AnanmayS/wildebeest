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
