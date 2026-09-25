import type { Benchmarks, SystemSnapshot } from '../../lib/types';
import { fmtAgo, fmtInt } from '../../lib/format';
import { Check } from '../pipeline/Nodes';
import { Card } from '../ui/Card';

const CHECKS = [
  { key: 'duplicateResults', label: 'Duplicate results', what: 'images with more than one result row' },
  { key: 'stuckLeases', label: 'Stuck leases', what: 'tasks still leased to a dead worker after timeout + 2 reaper ticks' },
  { key: 'lostImages', label: 'Lost images', what: 'images with no task and no final category' },
] as const;

/** Live safety checks the coordinator runs every few seconds, plus the offline fault-injection record. */
export function Invariants({ invariants, faults, now }: { invariants?: SystemSnapshot['invariants']; faults?: Benchmarks['faults']; now: number }) {
  if (!invariants && !faults) return null;
  return (
    <Card
      title="Invariants"
      caption="Checked live against Postgres every few seconds."
      aside={invariants ? `checked ${fmtAgo(invariants.checkedAt, now)}` : undefined}
    >
      {invariants && (
        <ul className="flex flex-col gap-1.5">
          {CHECKS.map((c) => {
            const n = invariants[c.key];
            const ok = n === 0;
            return (
              <li
                key={c.key}
                title={`${c.label}: ${c.what}`}
                className={`flex items-center gap-2 rounded-md border px-2.5 py-1 ${ok ? 'border-leaf-400/25 text-leaf-300' : 'border-ember-400/60 bg-ember-400/10 text-ember-300'}`}
              >
                <Check ok={ok} />
                <span className="flex-1 text-[13px] text-ink-100">{c.label}</span>
                <span className="text-[20px] font-semibold leading-none tabular">{fmtInt(n)}</span>
              </li>
            );
          })}
        </ul>
      )}
      {faults && (
        <p className="mt-2 text-[12px] leading-snug text-ink-400">
          Fault matrix: <b className="font-semibold text-ink-100">{fmtInt(faults.faultsInjected)}</b> faults over {fmtInt(faults.runs)} runs,{' '}
          <b className={`font-semibold ${faults.violations ? 'text-ember-300' : 'text-leaf-300'}`}>{fmtInt(faults.violations)} violations</b>.
        </p>
      )}
    </Card>
  );
}
