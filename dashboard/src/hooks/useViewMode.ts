import { useCallback, useEffect, useState } from 'react';

export type ViewMode = 'story' | 'engineer';

const KEY = 'wildebeest.view';

function fromHash(): ViewMode | null {
  const h = location.hash.replace('#', '');
  return h === 'story' || h === 'engineer' ? h : null;
}

function stored(): ViewMode | null {
  try {
    const v = localStorage.getItem(KEY);
    return v === 'story' || v === 'engineer' ? v : null;
  } catch {
    return null;
  }
}

/**
 * Story (plain-language, the default) or Engineer (the full system view). A `#story` / `#engineer`
 * link wins over the remembered choice; switching updates both the hash and localStorage.
 */
export function useViewMode(): [ViewMode, (m: ViewMode) => void] {
  const [mode, setModeState] = useState<ViewMode>(() => fromHash() ?? stored() ?? 'story');

  useEffect(() => {
    const onHash = () => {
      const h = fromHash();
      if (h) setModeState(h);
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  const setMode = useCallback((m: ViewMode) => {
    setModeState(m);
    try {
      localStorage.setItem(KEY, m);
    } catch {
      /* private mode: the hash still carries it */
    }
    history.replaceState(null, '', `#${m}`);
  }, []);

  return [mode, setMode];
}
