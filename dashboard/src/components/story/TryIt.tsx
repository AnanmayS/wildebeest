import { DEMO_SIZE, type Demo } from '../../hooks/useDemo';
import { narrate, type Episode, type Line, type NarrationContext, type Tone } from '../../lib/narration';
import { fmtAgo } from '../../lib/format';

interface Props {
  demo: Demo;
  ctx: NarrationContext;
}

/** The three demo buttons and, below them, the story of whatever the visitor just did. */
export function TryIt({ demo, ctx }: Props) {
  return (
    <section className="card flex min-h-0 flex-col px-5 pb-4 pt-4">
      <h2 className="text-[22px] font-semibold tracking-tight">Try it</h2>
      <p className="mt-1 text-[13.5px] leading-snug text-ink-400">Live on this laptop: real AI models, real crashes.</p>

      <div className="mt-4 flex flex-col gap-2.5">
        <button className="btn btn-primary h-12 justify-center text-[16px]" disabled={!demo.canRun} onClick={demo.run}>
          {demo.starting ? 'Starting…' : demo.running ? 'Demo running…' : 'Run a live demo'}
        </button>
        <p className="-mt-1 text-center text-[12.5px] text-ink-500">Sorts {DEMO_SIZE} sample photos</p>
        <div className="grid grid-cols-2 gap-2.5">
          <ActionButton label="Crash a worker" hint="Kill the busiest one" tone="ember" blocked={demo.blocked} onClick={demo.crash} />
          <ActionButton label="Freeze a worker" hint="Stuck for 20 s" tone="sun" blocked={demo.blocked} onClick={demo.freeze} />
        </div>
      </div>

      <h3 className="mt-5 text-[13px] font-semibold uppercase tracking-[0.14em] text-ink-400">What just happened</h3>
      <div className="scroll-thin mt-2 min-h-0 flex-1 overflow-y-auto pr-1" aria-live="polite">
        {demo.episodes.length === 0 ? (
          <p className="rounded-lg border border-dashed border-ink-700 px-4 py-6 text-center text-[14px] leading-relaxed text-ink-500">
            Start a demo, then crash or freeze a worker while it runs. Each step shows up here as it happens, with real timings.
          </p>
        ) : (
          demo.episodes.map((ep, i) => <EpisodeStory key={ep.at} ep={ep} ctx={ctx} latest={i === 0} />)
        )}
      </div>
    </section>
  );
}

function ActionButton({ label, hint, tone, blocked, onClick }: { label: string; hint: string; tone: 'ember' | 'sun'; blocked: string | null; onClick: () => void }) {
  const look = tone === 'ember'
    ? 'border-ember-400/60 text-ember-300 hover:bg-ember-400/10'
    : 'border-sun-400/60 text-sun-300 hover:bg-sun-400/10';
  return (
    <button
      className={`flex h-auto flex-col items-center rounded-lg border bg-ink-850 px-3 py-2.5 transition disabled:cursor-not-allowed disabled:opacity-40 ${look}`}
      disabled={!!blocked}
      title={blocked ?? undefined}
      onClick={onClick}
    >
      <span className="text-[15px] font-semibold">{label}</span>
      <span className="text-[12px] text-ink-400">{blocked ?? hint}</span>
    </button>
  );
}

const DOT: Record<Tone, string> = {
  ember: 'bg-ember-400', sun: 'bg-sun-400', violet: 'bg-violet-400', leaf: 'bg-leaf-400', ink: 'bg-ink-500',
};
const EM: Record<Tone, string> = {
  ember: 'text-ember-300', sun: 'text-sun-300', violet: 'text-violet-300', leaf: 'text-leaf-300', ink: 'text-ink-100',
};

/** One action's story as a short timeline. Older ones fade so the newest reads first. */
function EpisodeStory({ ep, ctx, latest }: { ep: Episode; ctx: NarrationContext; latest: boolean }) {
  const lines = narrate(ep, ctx);
  return (
    <div className={`mb-4 ${latest ? '' : 'opacity-55'}`}>
      <div className="mb-1.5 text-[11.5px] uppercase tracking-[0.12em] text-ink-500">{fmtAgo(ep.at, ctx.now)}</div>
      <ol className="relative flex flex-col gap-2 border-l border-ink-700 pl-4">
        {lines.map((l, i) => <NarrationLine key={i} line={l} />)}
      </ol>
    </div>
  );
}

function NarrationLine({ line }: { line: Line }) {
  return (
    <li className={`relative animate-rise text-[15px] leading-snug ${line.pending ? 'text-ink-400' : 'text-ink-300'}`}>
      <span className={`absolute -left-[21px] top-[6px] h-2.5 w-2.5 rounded-full ring-4 ring-ink-900 ${DOT[line.tone]} ${line.pending ? 'animate-pulse-dot' : ''}`} />
      {line.parts.map((p, i) => (typeof p === 'string' ? p : <b key={i} className={`font-semibold ${EM[line.tone]}`}>{p.em}</b>))}
    </li>
  );
}
