import { useCallback, useRef, useState } from 'react';

export type PacketKind = 'fwd' | 'back';
interface Packet { id: number; kind: PacketKind }

/** Packets in flight on one wire. `spawn` is called from real lease changes only. */
export function usePackets() {
  const [packets, setPackets] = useState<Packet[]>([]);
  const seq = useRef(0);
  const spawn = useCallback((kind: PacketKind) => {
    // Cap so a burst of fast tasks can't pile up hundreds of DOM nodes.
    setPackets((cur) => (cur.length >= 4 ? cur : [...cur, { id: seq.current++, kind }]));
  }, []);
  const remove = useCallback((id: number) => setPackets((cur) => cur.filter((p) => p.id !== id)), []);
  return { packets, spawn, remove };
}

interface WireProps {
  packets?: Packet[];
  onDone?: (id: number) => void;
  className?: string;
  /** Draw an arrowhead at the right end. */
  arrow?: boolean;
  tone?: string;
}

/** A 1px connector; packets ride on it left→right (fwd) or right→left (back, a requeue). */
export function Wire({ packets = [], onDone, className = '', arrow = false, tone = 'bg-ink-600' }: WireProps) {
  return (
    <div className={`relative h-px self-center ${tone} ${className}`}>
      {arrow && <span className="absolute -right-px -top-[3px] h-0 w-0 border-y-[3.5px] border-l-[5px] border-y-transparent border-l-ink-500" />}
      {packets.map((p) => (
        <span key={p.id} className={`packet packet-${p.kind}`} onAnimationEnd={() => onDone?.(p.id)} />
      ))}
    </div>
  );
}
