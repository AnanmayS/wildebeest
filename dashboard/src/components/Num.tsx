import { useTween } from '../hooks/useTween';
import { fmtInt } from '../lib/format';

/** A live counter that rolls to its new value. */
export function Num({ value, className = '' }: { value: number; className?: string }) {
  const shown = useTween(value);
  return <span className={`tabular ${className}`}>{fmtInt(shown)}</span>;
}
