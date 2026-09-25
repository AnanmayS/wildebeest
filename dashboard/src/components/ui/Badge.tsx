import type { ReactNode } from 'react';

const TONES = {
  ink: 'border-ink-700 text-ink-300',
  leaf: 'border-leaf-400/40 text-leaf-300',
  ember: 'border-ember-400/50 text-ember-300',
  sun: 'border-sun-400/50 text-sun-300',
  violet: 'border-violet-400/50 text-violet-300',
  sky: 'border-sky-400/50 text-sky-400',
};

/** Small outlined tag: device, runtime, mode. Outline, not fill, so it never competes with data. */
export function Badge({ tone = 'ink', children, title }: { tone?: keyof typeof TONES; children: ReactNode; title?: string }) {
  return (
    <span title={title} className={`inline-flex h-[18px] shrink-0 items-center rounded border px-1.5 text-[10px] font-semibold uppercase tracking-[0.08em] ${TONES[tone]}`}>
      {children}
    </span>
  );
}
