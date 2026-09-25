import type { ReactNode } from 'react';

interface Props {
  title: ReactNode;
  /** One plain-language sentence: what this panel proves about the system. */
  caption?: ReactNode;
  aside?: ReactNode;
  className?: string;
  children: ReactNode;
}

export function Card({ title, caption, aside, className = '', children }: Props) {
  return (
    <section className={`card flex min-h-0 min-w-0 flex-col px-4 pb-3 pt-2.5 ${className}`}>
      <header className="flex items-baseline justify-between gap-3">
        <h3 className="text-[15px] font-semibold tracking-tight text-ink-100">{title}</h3>
        {aside && <div className="shrink-0 text-xs text-ink-400">{aside}</div>}
      </header>
      {caption && <p className="caption mt-0.5">{caption}</p>}
      <div className="mt-2 min-h-0 flex-1">{children}</div>
    </section>
  );
}
