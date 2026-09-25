import type { Speculation, TaskEvent } from '../../lib/types';
import { fmtInt, fmtMs, shortId, shortTask } from '../../lib/format';

interface Props {
  speculation: Speculation;
  events: TaskEvent[];
}

const RULE =
  'When a stage has nothing left to hand out and a worker sits idle, a task running longer than 3× the stage p50 ' +
  'gets one speculative copy on the fastest idle worker. The first result to commit wins; the other attempt is told to cancel ' +
  '(409 ALREADY_DONE if it reports anyway). Workers slower than 3× the stage p50 are on probation: they keep their own work ' +
  'but are never given copies.';

/**
 * Straggler speculation in one line: how many copies were launched and how many beat the
 * original, plus the latest copy's story. This is what keeps one slow worker from setting the tail.
 */
export function StragglerStrip({ speculation: s, events }: Props) {
  const last = events.find((e) => e.type === 'speculated');
  const outcome = last && events.find((e) => (e.type === 'speculation_won' || e.type === 'speculation_wasted') && e.taskId === last.taskId);
  const d = last?.detail ?? {};
  const age = typeof d.ageMs === 'number' ? d.ageMs : null;
  const threshold = typeof d.thresholdMs === 'number' ? d.thresholdMs : null;
  const probation = s.probation ?? [];

  return (
    <section className="card flex h-[38px] min-w-0 items-center gap-3 px-4 text-[13px]" title={RULE}>
      <h3 className="shrink-0 text-[14px] font-semibold tracking-tight text-ink-100">Stragglers</h3>
      {s.launched === 0 && !s.running ? (
        <p className="min-w-0 truncate text-ink-400">
          no copies needed yet · a task running over 3× its stage p50 gets a copy on the fastest idle worker
        </p>
      ) : (
        <p className="flex min-w-0 items-baseline gap-1.5 truncate text-ink-400">
          <b className="text-[17px] font-semibold leading-none text-sky-400">{fmtInt(s.won)}</b>
          <span>of {fmtInt(s.launched)} copies won</span>
          <span className="text-ink-600">·</span>
          <span>{fmtInt(s.wasted)} wasted</span>
          {!!s.running && (
            <>
              <span className="text-ink-600">·</span>
              <span className="text-sky-400">{fmtInt(s.running)} running</span>
            </>
          )}
          {last && (
            <span className="truncate text-ink-500" title={last.message}>
              {' '}· latest: <span className="font-mono">{shortTask(last.taskId ?? '')}</span>
              {age != null && <> {fmtMs(age)}</>}
              {threshold != null && <> &gt; {fmtMs(threshold)}</>}
              {' '}→ <span className="font-mono">{shortId(last.workerId ?? '?').slice(0, 6)}</span>
              {outcome ? (outcome.type === 'speculation_won' ? ', won' : ', wasted') : ''}
            </span>
          )}
        </p>
      )}
      {probation.length > 0 && (
        <span
          className="ml-auto inline-flex h-[20px] shrink-0 items-center rounded border border-sun-400/50 px-1.5 text-[11px] font-semibold text-sun-300"
          title={probation.map((p) => `${p.workerId}: p50 ${fmtMs(p.p50ServiceMs)} vs stage p50 ${fmtMs(p.stageP50ServiceMs)}`).join('\n')}
        >
          {probation.length} on probation
        </span>
      )}
    </section>
  );
}
