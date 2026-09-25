import type { HaView } from '../../lib/ha';

/**
 * The coordinator replicas, one pill each: the leader (runs the sweep, reaper and death watch)
 * with its term, the standby, and any replica that stopped answering. Both serve the API.
 */
export function Coordinators({ ha }: { ha: HaView }) {
  const silent = ha.leaderSilent;
  return (
    <div className="flex shrink-0 items-center gap-1.5" aria-label="Coordinator replicas">
      <span className="text-[10.5px] uppercase tracking-[0.12em] text-ink-500">coordinators</span>
      {ha.nodes.map((n) => {
        const isSilentLeader = silent?.id === n.id;
        const tone = isSilentLeader
          ? 'border-sun-400/60 text-sun-300'
          : n.role === 'leader' ? 'border-leaf-400/50 text-leaf-300' : n.role === 'down' ? 'border-ember-400/50 text-ember-300' : 'border-ink-700 text-ink-300';
        const dot = isSilentLeader ? 'bg-sun-400 animate-pulse-dot' : n.role === 'leader' ? 'bg-leaf-400' : n.role === 'down' ? 'bg-ember-400' : 'border border-ink-400';
        const label = isSilentLeader
          ? <>silent {Math.floor(silent!.forMs / 1000)} s{silent!.leaseMs ? ` / ${Math.round(silent!.leaseMs / 1000)} s lease` : ''}</>
          : n.role === 'leader' ? <>leader · term <b className="font-semibold">{n.term ?? ha.leader?.term}</b></> : n.role === 'down' ? 'down' : 'standby';
        const title = isSilentLeader
          ? `${n.id} stopped renewing its leader lease (term ${silent!.term}). When the lease expires a standby takes over with term ${silent!.term + 1}.`
          : n.role === 'leader' ? `${n.id} holds the leader lease (term ${n.term ?? ha.leader?.term}${ha.leader?.since ? `, since ${new Date(ha.leader.since).toLocaleTimeString('en-GB')}` : ''}): it runs the repair sweep, reaper, death watch and chaos. Both replicas serve the API.`
          : n.role === 'down' ? `${n.id} stopped heartbeating; the load balancer routes around it.`
          : `${n.id} serves the API and takes over if the leader's lease expires.`;
        return (
          <span key={n.id} title={title} className={`inline-flex h-[22px] items-center gap-1.5 rounded-md border px-2 text-[12px] ${tone}`}>
            <span className={`h-2 w-2 shrink-0 rounded-full ${dot}`} aria-hidden />
            <span className="font-mono text-ink-100">{n.id}</span>
            <span>{label}</span>
          </span>
        );
      })}
    </div>
  );
}
