// The Story view's "what just happened": each demo action the visitor took, told in plain words
// from the coordinator's real events and snapshot, with real timings. Nothing here is scripted
// ahead of the data: a step appears only when the event behind it has arrived.

import type { LiveJob } from '../hooks/useWildebeest';
import { epochsFromMessage } from './workerStory';
import { latestRecovery, type LeaseSightings, type RecoveryView } from './recovery';
import { fmtSeconds } from './proof';
import { fmtSec } from './format';
import type { SystemSnapshot, TaskEvent, Worker } from './types';

export type Episode =
  | { kind: 'run'; at: number; jobId: string; size: number }
  | { kind: 'crash'; at: number; workerId: string }
  | { kind: 'freeze'; at: number; workerId: string; ms: number };

export type Tone = 'ember' | 'sun' | 'violet' | 'leaf' | 'ink';
/** Plain text with emphasised bits (names, timings). */
export type Part = string | { em: string };
export interface Line { parts: Part[]; tone: Tone; pending?: boolean }

export interface NarrationContext {
  job: LiveJob | null;
  /** The last state seen of every job this page started, so an older run still reads correctly. */
  jobs: ReadonlyMap<string, LiveJob>;
  workers: Worker[];
  events: TaskEvent[]; // newest first
  system: SystemSnapshot | null;
  sightings: LeaseSightings;
  workerTimeoutMs: number;
  now: number;
}

/** "13199c" for a container (first 6 of its id), "aro.local" for a native worker. */
export function workerName(id: string): string {
  const bare = id.replace(/^(detect|classify)-/, '');
  return /^[0-9a-f]{12}$/.test(bare) ? bare.slice(0, 6) : bare;
}

export function narrate(ep: Episode, ctx: NarrationContext): Line[] {
  if (ep.kind === 'run') return runLines(ep, ctx);
  if (ep.kind === 'crash') return crashLines(ep, ctx);
  return freezeLines(ep, ctx);
}

const em = (s: string | number): Part => ({ em: String(s) });
const plural = (n: number, one: string, many = `${one}s`) => (n === 1 ? one : many);

/** "in 0.1 s", or "in under 0.1 s" for a cached job whose clock reads 0. */
const took = (ms: number) => (ms < 100 ? 'in under 0.1 s' : `in ${fmtSeconds(ms)}`);

function runLines(ep: Extract<Episode, { kind: 'run' }>, ctx: NarrationContext): Line[] {
  const lines: Line[] = [{ tone: 'ink', parts: ['Sent ', em(ep.size), ' sample camera photos in.'] }];
  const job = ctx.job?.id === ep.jobId ? ctx.job : ctx.jobs.get(ep.jobId);
  if (!job) return [...lines, { tone: 'ink', pending: true, parts: ['Starting…'] }];

  if (job.status === 'running') {
    lines.push({
      tone: 'leaf', pending: true,
      parts: ['Sorting: ', em(`${job.processed} of ${job.total}`), ' done', job.cacheHits ? `, ${job.cacheHits} already known from an earlier run` : ''],
    });
    return lines;
  }
  if (job.status === 'cancelled') return [...lines, { tone: 'ink', parts: ['The job was cancelled.'] }];

  const c = job.categories;
  if (job.total > 0 && job.cacheHits === job.total) {
    lines.push({ tone: 'leaf', parts: [`All ${job.total} were already sorted: cached results come back `, em(took(job.elapsedMs)), '.'] });
    lines.push({ tone: 'ink', parts: ['Nothing had to be computed, so there was no work in progress to crash this time.'] });
    return lines;
  }
  lines.push({
    tone: 'leaf',
    parts: ['Done: ', em(`${job.total} photos sorted ${took(job.elapsedMs)}`), `: ${c.empty} empty, ${c.animal} with animals, ${c.human + c.vehicle} with people or vehicles.`],
  });
  if (c.failed) lines.push({ tone: 'ember', parts: [`${c.failed} ${plural(c.failed, 'photo')} could not be read.`] });
  return lines;
}

/** A demo request that the page itself logged as failed (e.g. the kill endpoint refused). */
function failure(ep: { at: number; workerId: string }, events: TaskEvent[]): Line | null {
  const f = events.find((e) => e.type === 'failed' && e.id < 0 && e.workerId === ep.workerId && Date.parse(e.at) >= ep.at - 1000);
  return f ? { tone: 'ember', parts: [f.message] } : null;
}

function crashLines(ep: Extract<Episode, { kind: 'crash' }>, ctx: NarrationContext): Line[] {
  const name = workerName(ep.workerId);
  const lines: Line[] = [{ tone: 'ember', parts: ['Crashed worker ', em(name), ' on purpose, like pulling its plug.'] }];
  const failed = failure(ep, ctx.events);
  if (failed) return [...lines, failed];

  const rv = latestRecovery(ctx.system, ctx.events, ctx.sightings, { workerId: ep.workerId, since: ep.at - 2000 });
  if (!rv) {
    const waited = ctx.now - ep.at;
    return [...lines, {
      tone: 'ink', pending: true,
      parts: waited < 10_000 ? ['Waiting for Docker to report it…'] : [`No word from Docker yet; missed check-ins will give it away within ${fmtSec(ctx.workerTimeoutMs)}.`],
    }];
  }

  const noticeMs = rv.detectedAt != null ? (rv.via === 'docker_event' ? rv.exitToDeadMs ?? rv.detectedAt - rv.killedAt : rv.detectedAt - rv.killedAt) : null;
  if (noticeMs != null) {
    lines.push(rv.via === 'docker_event'
      ? { tone: 'ember', parts: ['Noticed in ', em(fmtSeconds(noticeMs)), `: Docker told us, instead of waiting ${fmtSec(ctx.workerTimeoutMs)} for missed check-ins.`] }
      : { tone: 'ember', parts: ['Noticed after ', em(fmtSeconds(noticeMs)), ' of missed check-ins.'] });
  }
  return [...lines, ...handover(rv, ctx, 'crash', crashedAt(rv, ctx.events))];
}

/**
 * When the container actually exited (the die event's exitedAt), else when the kill was sent. Docker Desktop
 * can take a second or two to carry out a kill under load; that is Docker's time, not the system's.
 */
function crashedAt(rv: RecoveryView, events: TaskEvent[]): number {
  const died = events.find((e) => e.type === 'worker_died' && e.workerId === rv.workerId && rv.detectedAt != null && Math.abs(Date.parse(e.at) - rv.detectedAt) < 2000);
  const exited = typeof died?.detail?.exitedAt === 'string' ? Date.parse(died.detail.exitedAt) : NaN;
  return Number.isFinite(exited) && exited >= rv.killedAt ? exited : rv.killedAt;
}

/** Who took over the dead worker's photo, how long until it was running again, and the loss check. */
function handover(rv: RecoveryView, ctx: NarrationContext, cause: 'crash' | 'freeze', from = rv.killedAt): Line[] {
  const lines: Line[] = [];
  if (rv.tasks === 0) {
    lines.push(rv.promotedTo
      ? { tone: 'sun', parts: ['A backup copy already running on ', em(workerName(rv.promotedTo)), ' took over its photo.'] }
      : { tone: 'ink', parts: ["It wasn't in the middle of a photo, so nothing needed handing over."] });
  } else {
    const photos = rv.tasks === 1 ? 'Its photo was' : `Its ${rv.tasks} photos were`;
    if (rv.reclaimedAt == null) {
      lines.push({ tone: 'sun', pending: true, parts: [`${photos} put back at the front of the line for the next free worker…`] });
      return lines;
    }
    lines.push({ tone: 'sun', parts: [`${photos} handed to `, rv.reclaimedBy ? em(workerName(rv.reclaimedBy)) : 'another worker', '.'] });
    lines.push({ tone: 'leaf', parts: ['Back to work ', em(fmtSeconds(rv.reclaimedAt - from)), ` after the ${cause}.`] });
  }
  // After a freeze the loss check waits until the frozen worker has woken and been dealt with.
  if (cause === 'crash') lines.push(lossCheck(rv, ctx));
  return lines;
}

/** The coordinator's live invariant check, once it has run after the recovery. */
function lossCheck(rv: RecoveryView, { system, job }: NarrationContext): Line {
  const inv = system?.invariants;
  const after = rv.reclaimedAt ?? rv.detectedAt ?? rv.killedAt;
  if (inv && Date.parse(inv.checkedAt) >= after) {
    const ok = inv.lostImages === 0 && inv.duplicateResults === 0;
    return { tone: ok ? 'leaf' : 'ember', parts: ['Photos lost: ', em(inv.lostImages), ' · counted twice: ', em(inv.duplicateResults)] };
  }
  if (!inv && job && job.status === 'done') {
    return { tone: 'leaf', parts: ['Job finished with ', em(`${job.processed} of ${job.total}`), ' photos sorted.'] };
  }
  return { tone: 'ink', pending: true, parts: ['Checking that every photo is accounted for…'] };
}

function freezeLines(ep: Extract<Episode, { kind: 'freeze' }>, ctx: NarrationContext): Line[] {
  const { events, now } = ctx;
  const name = workerName(ep.workerId);
  const mine = (type: string, since: number) =>
    events.find((e) => e.type === type && e.workerId === ep.workerId && Date.parse(e.at) >= since);
  const lines: Line[] = [{
    tone: 'sun',
    parts: ['Froze worker ', em(name), ` for ${Math.round(ep.ms / 1000)} s. It's still there, just stuck, like a computer that hangs.`],
  }];
  const failed = failure(ep, events);
  if (failed) return [...lines, failed];

  const paused = mine('worker_paused', ep.at - 2000);
  const pausedAt = paused ? Date.parse(paused.at) : ep.at;
  const wakeAt = pausedAt + ep.ms;
  const died = mine('worker_died', pausedAt);

  if (!died) {
    if (now < wakeAt) {
      lines.push({ tone: 'ink', pending: true, parts: ['Waiting to see it miss its check-ins… ', em(`${Math.floor((now - pausedAt) / 1000)} s`)] });
      return lines;
    }
  } else {
    lines.push({ tone: 'ember', parts: ['Missed its check-ins for ', em(fmtSeconds(Date.parse(died.at) - pausedAt)), ', so it was declared dead.'] });
    const rv = latestRecovery(ctx.system, events, ctx.sightings, { workerId: ep.workerId, since: pausedAt });
    if (rv) lines.push(...handover(rv, ctx, 'freeze', pausedAt));
  }

  if (now < wakeAt) {
    lines.push({ tone: 'ink', pending: true, parts: ['It wakes up in ', em(`${Math.ceil((wakeAt - now) / 1000)} s`), '…'] });
    return lines;
  }

  const stale = mine('stale_rejected', pausedAt);
  const refused = mine('heartbeat_refused', pausedAt);
  const zombie = stale && isRealFencing(stale);
  if (zombie) {
    lines.push({
      tone: 'violet',
      parts: ['When it woke up it tried to hand in an old answer; the system ', em('rejected it'), ' because the photo had already been given to someone else. No double counting.'],
    });
  } else if (refused && (now - Date.parse(refused.at) > 4000 || !heldAny(refused))) {
    lines.push({
      tone: 'violet',
      parts: ['When it woke up, the system told it it had ', em('been replaced'), ', so it threw its old answer away and rejoined. No double counting.'],
    });
  } else if (died) {
    lines.push({ tone: 'ink', pending: true, parts: ['Waking up…'] });
    return lines;
  }

  const w = ctx.workers.find((x) => x.id === ep.workerId);
  if (w?.status === 'ALIVE') lines.push({ tone: 'leaf', parts: ["It's back and taking new photos."] });
  if (died) {
    const rv = latestRecovery(ctx.system, events, ctx.sightings, { workerId: ep.workerId, since: pausedAt });
    if (rv) lines.push(lossCheck(rv, ctx));
  }
  return lines;
}

/** A stale result from an older lease (epoch below the task's current one), not a re-sent duplicate. */
function isRealFencing(e: TaskEvent): boolean {
  const parsed = epochsFromMessage(e.message);
  const epoch = typeof e.detail?.leaseEpoch === 'number' ? e.detail.leaseEpoch : parsed.epoch;
  const current = typeof e.detail?.currentEpoch === 'number' ? e.detail.currentEpoch : parsed.currentEpoch;
  return epoch == null || epoch !== current;
}

const heldAny = (refused: TaskEvent) => Array.isArray(refused.detail?.heldTaskIds) && (refused.detail.heldTaskIds as unknown[]).length > 0;
