import type { ReactNode } from 'react';
import { fmtInt, fmtSec } from '../../lib/format';
import { fmtFactor, fmtSeconds, type Proof } from '../../lib/proof';

/** Three measured results, each with one plain sentence on what it means. */
export function ProofNumbers({ proof, workerTimeoutMs }: { proof: Proof; workerTimeoutMs: number }) {
  const { recovery: r, speedup: s, faults: f } = proof;
  return (
    <section className="grid grid-cols-3 gap-3.5" aria-label="Measured results">
      <ProofCard
        tone="text-leaf-300"
        big={fmtSeconds(r.afterMs)}
        was={`was ${fmtSeconds(r.beforeMs)}`}
        title="to recover from a crash"
      >
        Docker reports a dead worker at once, so its photo moves on without waiting {fmtSec(workerTimeoutMs)} for missed check-ins.
        {r.samples ? ` Median of ${r.samples} test crashes.` : ''}
      </ProofCard>
      <ProofCard tone="text-leaf-300" big={fmtFactor(s.factor)} title="more work per second">
        {fmtInt(s.afterPerSec)} tasks a second vs {fmtInt(s.beforePerSec)} in the first version, timed with instant stand-in tasks to test the
        scheduling, not the AI.
      </ProofCard>
      <ProofCard tone={f.violations ? 'text-ember-300' : 'text-leaf-300'} big={fmtInt(f.violations)} title="photos lost or counted twice">
        after {fmtInt(f.faults)} failures injected on purpose (crashes, freezes, network and storage outages) over {fmtInt(f.runs)} test runs.
      </ProofCard>
    </section>
  );
}

function ProofCard({ big, was, title, tone, children }: { big: string; was?: string; title: string; tone: string; children: ReactNode }) {
  return (
    <div className="card px-5 pb-3.5 pt-3.5">
      <div className="flex items-baseline gap-3">
        <span className={`text-[46px] font-semibold leading-none tracking-tight tabular ${tone}`}>{big}</span>
        {was && <span className="text-[16px] text-ink-500 line-through decoration-ink-600">{was}</span>}
      </div>
      <h3 className="mt-1.5 text-[18px] font-semibold leading-tight">{title}</h3>
      <p className="mt-1 text-[13.5px] leading-snug text-ink-400">{children}</p>
    </div>
  );
}
