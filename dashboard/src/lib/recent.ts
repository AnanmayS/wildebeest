// "Recent" for failures and failovers: during this page session, or in the last ~10 minutes.
// Older records (e.g. a native worker that went quiet during a test restart hours ago) are history,
// not the story; they stay in the event log.

export const RECENT_MS = 10 * 60_000;
export const PAGE_OPENED_AT = Date.now();

/** Anything at or after this moment counts as recent. */
export const recentSince = (now: number) => Math.min(PAGE_OPENED_AT, now - RECENT_MS);

export const isRecent = (at: number, now: number) => at >= recentSince(now);
