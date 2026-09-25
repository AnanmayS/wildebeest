/** 2×2 grid mark: two lit cells (live workers), two dim ones. */
export function Logo() {
  return (
    <svg viewBox="0 0 32 32" className="h-9 w-9 shrink-0" aria-hidden>
      <rect width="32" height="32" rx="8" className="fill-ink-850 stroke-ink-700" />
      <g className="fill-leaf-400">
        <rect x="7" y="7" width="7.5" height="7.5" rx="1.8" />
        <rect x="17.5" y="7" width="7.5" height="7.5" rx="1.8" opacity=".35" />
        <rect x="7" y="17.5" width="7.5" height="7.5" rx="1.8" opacity=".35" />
        <rect x="17.5" y="17.5" width="7.5" height="7.5" rx="1.8" />
      </g>
    </svg>
  );
}
