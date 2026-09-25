import type { Failover, HaView } from '../../lib/ha';
import { fmtAgo, fmtInt, fmtMs } from '../../lib/format';

interface Props {
  failover: Failover | null;
  silent: HaView['leaderSilent'];
  now: number;
}

/**
 * One line about the coordinators: a failover in progress (leader silent, lease running out),
 * or the latest completed one. Hidden until either exists: the pipeline header shows the steady state.
 */
export function FailoverStrip({ failover: f, silent, now }: Props) {
  if (!f && !silent) return null;
  // A new silence outranks the record of an older failover.
  const live = silent && (!f || f.term <= silent.term);
  return (
    <section
      className={`card flex h-[38px] min-w-0 items-center gap-3 px-4 text-[13px] ${live ? 'border-sun-400/40' : ''}`}
      title={f && !live ? detail(f) : undefined}
    >
      <h3 className="shrink-0 text-[14px] font-semibold tracking-tight text-ink-100">Leader failover</h3>
      {live ? (
        <p className="min-w-0 truncate text-sun-300">
          leader <span className="font-mono">{silent!.id}</span> silent {Math.floor(silent!.forMs / 1000)} s
          <span className="text-ink-400">
            {' '}· when its {silent!.leaseMs ? fmtMs(silent!.leaseMs) : ''} lease expires the standby takes term {silent!.term + 1}; workers keep going
          </span>
        </p>
      ) : (
        <>
          <p className="min-w-0 truncate text-ink-300">
            {f!.from === f!.to ? (
              // Same replica id, new process (a restart): it resigned, then won the next election.
              <><span className="font-mono text-leaf-300">{f!.to}</span> restarted, re-elected, term <b className="font-semibold text-ink-100">{f!.term}</b></>
            ) : (
              <>
                <span className="font-mono text-ember-300">{f!.from}</span> lost →{' '}
                <span className="font-mono text-leaf-300">{f!.to}</span> elected, term <b className="font-semibold text-ink-100">{f!.term}</b>
              </>
            )}
            {f!.totalMs != null && <>, in <b className="text-[15px] font-semibold text-ink-100">{fmtMs(f!.totalMs)}</b></>}
            <span className="text-ink-400">
              {' '}· <b className={`font-semibold ${f!.fencedWrites ? 'text-violet-300' : 'text-ink-100'}`}>{fmtInt(f!.fencedWrites)}</b> stale-term write{f!.fencedWrites === 1 ? '' : 's'}{f!.fencedWrites ? ' refused' : ''}
            </span>
          </p>
          <span className="ml-auto shrink-0 text-xs text-ink-400">{fmtAgo(f!.at, now)}</span>
        </>
      )}
    </section>
  );
}

function detail(f: Failover): string {
  const parts = [
    `${f.from} (term ${f.fromTerm ?? '?'}) lost the lead${f.reason ? ` (${f.reason})` : ''} → ${f.to} (term ${f.term})`,
    f.totalMs != null ? `${fmtMs(f.totalMs)} from ${f.from}'s last lease renewal to ${f.to} finishing its first sweep` : null,
    f.leaderlessMs != null ? `no leader for ${fmtMs(f.leaderlessMs)} (waiting out the lease)` : null,
    f.reconcileMs != null ? `reconcile + first sweep ${fmtMs(f.reconcileMs)}` : null,
    f.firstSweep ? `first sweep: ${f.firstSweep.pushed ?? 0} pushed, ${f.firstSweep.dead ?? 0} dead workers, ${f.firstSweep.requeued ?? 0} requeued` : null,
    `${f.fencedWrites} writes from the old term: each one would have been refused by the term guard (leader_fenced)`,
  ];
  return parts.filter(Boolean).join('\n');
}
