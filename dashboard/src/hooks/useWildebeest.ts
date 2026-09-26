import { useCallback, useEffect, useRef, useState } from 'react';
import { api, ApiError } from '../lib/api';
import type { Chaos, Cluster, Config, JobSummary, ServerMessage, SystemSnapshot, TaskEvent, Worker } from '../lib/types';

const POLL_MS = 2000;
const MAX_EVENTS = 200;
/** Polled data is only applied if the WebSocket hasn't sent that kind of update recently. */
const WS_FRESH_MS = 4000;
/** An older coordinator answers 404 for /system; ask again this rarely in case it gets upgraded. */
const SYSTEM_RETRY_MS = 30_000;
export const PAUSE_MS = 20_000;
/** Used until GET /config answers (or if it doesn't exist): the coordinator's documented defaults. */
const DEFAULT_CONFIG: Config = {
  animalConfThreshold: 0.2, heartbeatMs: 2000, workerTimeoutMs: 6000, leaseMs: 15000, humanReviewSecondsPerImage: 3,
};

export type Connection = 'live' | 'reconnecting';
export type LiveJob = JobSummary & { receivedAt: number };
/** Actions the dashboard started, tracked until the coordinator's events confirm them. */
export interface PendingAction { at: number; ms?: number }

/**
 * All live state for the dashboard. Primary source is the /api/events WebSocket;
 * REST polling every 2 s is the fallback so the page keeps working if the socket drops,
 * and so a v1 coordinator (no `system` snapshot) still drives the page.
 */
export function useWildebeest() {
  const [job, setJob] = useState<LiveJob | null>(null);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [events, setEvents] = useState<TaskEvent[]>([]);
  const [system, setSystem] = useState<SystemSnapshot | null>(null);
  const [throttle, setThrottle] = useState({ throttled: false, classifyQueue: 0 });
  const [queues, setQueues] = useState<{ detect: number; classify: number } | null>(null);
  const [chaos, setChaosState] = useState<Chaos>({ enabled: false, killEverySec: 20 });
  const [connection, setConnection] = useState<Connection>('reconnecting');
  const [killRequested, setKillRequested] = useState<Record<string, PendingAction>>({});
  const [pauseRequested, setPauseRequested] = useState<Record<string, PendingAction>>({});
  const [config, setConfig] = useState<Config>(DEFAULT_CONFIG);
  const [cluster, setCluster] = useState<Cluster | null>(null);

  const wsLastSeen = useRef({ job: 0, workers: 0, throttle: 0, system: 0 });
  const systemMissingAt = useRef(0);
  const clusterMissingAt = useRef(0);
  const localEventId = useRef(-1);
  const wsOpen = useRef(false);

  /** The active job is the newest one; updates for it only move forward. */
  const acceptJob = useCallback((next: JobSummary, force = false) => {
    setJob((cur) => {
      const stamped = { ...next, receivedAt: Date.now() };
      if (!cur || force) return stamped;
      if (cur.id !== next.id) return Date.parse(next.createdAt) > Date.parse(cur.createdAt) ? stamped : cur;
      if (cur.status !== 'running' && next.status === 'running') return cur;
      return next.processed >= cur.processed ? stamped : cur;
    });
  }, []);

  const acceptSystem = useCallback((s: SystemSnapshot) => {
    setSystem(s);
    if (s.queues) {
      setQueues({ detect: s.queues.detect, classify: s.queues.classify });
      setThrottle({ throttled: s.queues.throttled, classifyQueue: s.queues.classify });
    }
  }, []);

  /** Merge events from any source (WS batch, GET /events seed, local notes): dedupe by id, newest on top. */
  const addEvents = useCallback((incoming: TaskEvent[]) => {
    setEvents((cur) => {
      const seen = new Set(cur.map((e) => e.id));
      const fresh = incoming.filter((e) => !seen.has(e.id));
      if (!fresh.length) return cur;
      return [...fresh, ...cur].sort(newestFirst).slice(0, MAX_EVENTS);
    });
  }, []);

  const seedEvents = useCallback(() => {
    api.listEvents(MAX_EVENTS).then(addEvents).catch(() => {
      /* older coordinator without GET /events: the log fills from the socket */
    });
  }, [addEvents]);

  const logLocal = useCallback(
    (type: string, message: string, workerId?: string) =>
      addEvents([{ id: localEventId.current--, at: new Date().toISOString(), type, message, workerId }]),
    [addEvents],
  );

  // --- WebSocket with exponential backoff -----------------------------------
  useEffect(() => {
    let ws: WebSocket | null = null;
    let attempt = 0;
    let retryTimer: number | undefined;
    let stopped = false;

    const handle = (msg: ServerMessage) => {
      const now = Date.now();
      switch (msg.type) {
        case 'system':
          wsLastSeen.current.system = now;
          acceptSystem(msg.system);
          break;
        case 'job_progress':
          wsLastSeen.current.job = now;
          acceptJob(msg.job);
          break;
        case 'worker_update':
          wsLastSeen.current.workers = now;
          setWorkers(msg.workers);
          break;
        case 'task_events':
          addEvents(msg.events);
          break;
        case 'throttle':
          wsLastSeen.current.throttle = now;
          setThrottle({ throttled: msg.throttled, classifyQueue: msg.classifyQueue });
          break;
      }
    };

    const connect = () => {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      ws = new WebSocket(`${proto}://${location.host}/api/events`);
      ws.onopen = () => {
        attempt = 0;
        wsOpen.current = true;
        setConnection('live');
        seedEvents(); // catch up on anything missed while disconnected (also the initial load)
      };
      ws.onmessage = (e) => {
        try {
          handle(JSON.parse(e.data));
        } catch {
          /* ignore malformed frames */
        }
      };
      ws.onclose = () => {
        wsOpen.current = false;
        setConnection('reconnecting');
        if (stopped) return;
        const delay = Math.min(10_000, 500 * 2 ** attempt++);
        retryTimer = window.setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      stopped = true;
      clearTimeout(retryTimer);
      ws?.close();
    };
  }, [acceptJob, acceptSystem, addEvents, seedEvents]);

  // --- REST polling fallback -------------------------------------------------
  useEffect(() => {
    let cancelled = false;
    const stale = (key: keyof typeof wsLastSeen.current) => Date.now() - wsLastSeen.current[key] > WS_FRESH_MS;

    const pollSystem = async () => {
      if (!stale('system') || Date.now() - systemMissingAt.current < SYSTEM_RETRY_MS) return;
      try {
        const s = await api.getSystem();
        if (!cancelled) acceptSystem(s);
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) {
          systemMissingAt.current = Date.now();
          if (!cancelled) setSystem(null);
        }
      }
    };

    const pollCluster = async () => {
      if (Date.now() - clusterMissingAt.current < SYSTEM_RETRY_MS) return;
      try {
        const c = await api.getCluster();
        if (!cancelled) setCluster(c);
      } catch (err) {
        // 404: a single-coordinator build. Anything else (a replica mid-failover) keeps the last answer.
        if (err instanceof ApiError && err.status === 404) {
          clusterMissingAt.current = Date.now();
          if (!cancelled) setCluster(null);
        }
      }
    };

    const poll = async () => {
      if (!wsOpen.current) seedEvents(); // socket down: keep the event log fresh from REST
      pollSystem();
      pollCluster();
      const [jobs, workersRes, metrics, chaosRes] = await Promise.allSettled([
        api.listJobs(), api.listWorkers(), api.getMetrics(), api.getChaos(),
      ]);
      if (cancelled) return;
      let newest: JobSummary | undefined;
      if (jobs.status === 'fulfilled' && jobs.value[0]) {
        newest = await api.getJob(jobs.value[0].id).catch(() => jobs.value[0]);
        if (cancelled) return;
        acceptJob(newest);
      }
      if (workersRes.status === 'fulfilled' && stale('workers')) setWorkers(workersRes.value);
      const m = metrics.status === 'fulfilled' ? metrics.value : undefined;
      if (stale('system') && m?.queues) setQueues({ detect: m.queues.detect ?? 0, classify: m.queues.classify ?? 0 });
      if (stale('throttle') && stale('system')) {
        // Prefer the job summary's backpressure fields; fall back to /metrics on older coordinators.
        const throttled = newest?.throttled ?? m?.throttled;
        const classifyQueue = newest?.classifyQueue ?? m?.queues?.classify;
        if (typeof throttled === 'boolean') {
          setThrottle((t) => ({ throttled, classifyQueue: classifyQueue ?? t.classifyQueue }));
        }
      }
      if (chaosRes.status === 'fulfilled') setChaosState(chaosRes.value);
    };

    poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [acceptJob, acceptSystem, seedEvents]);

  useEffect(() => {
    api.getConfig().then((c) => setConfig((d) => ({ ...d, ...c }))).catch(() => {});
  }, []);

  // A kill is "in flight" until the worker is marked DEAD; a pause until it has run its course.
  useEffect(() => {
    setKillRequested((k) => {
      const done = workers.filter((w) => w.status !== 'ALIVE' && k[w.id]).map((w) => w.id);
      return done.length ? done.reduce(withoutKey, k) : k;
    });
  }, [workers]);

  // --- Actions -----------------------------------------------------------------
  const startSample = useCallback(async (size: number, opts: { fresh?: boolean; random?: boolean } = {}): Promise<string> => {
    const { jobId } = await api.startSample(size, opts);
    acceptJob(await api.getJob(jobId), true);
    return jobId;
  }, [acceptJob]);

  const focusJob = useCallback(async (jobId: string) => {
    acceptJob(await api.getJob(jobId), true);
  }, [acceptJob]);

  const killWorker = useCallback(async (id: string) => {
    setKillRequested((k) => ({ ...k, [id]: { at: Date.now() } })); // the server logs a worker_killed event
    try {
      await api.killWorker(id);
    } catch (err) {
      setKillRequested((k) => withoutKey(k, id));
      logLocal('failed', `Kill ${id} failed: ${(err as Error).message}`, id);
    }
  }, [logLocal]);

  const pauseWorker = useCallback(async (id: string) => {
    const at = Date.now();
    setPauseRequested((p) => ({ ...p, [id]: { at, ms: PAUSE_MS } }));
    // Forget it once the pause, the wake-up and the fencing verdict have all had time to play out.
    window.setTimeout(() => setPauseRequested((p) => (p[id]?.at === at ? withoutKey(p, id) : p)), PAUSE_MS + 15_000);
    try {
      await api.pauseWorker(id, PAUSE_MS);
    } catch (err) {
      setPauseRequested((p) => withoutKey(p, id));
      logLocal('failed', `Pause ${id} failed: ${(err as Error).message}`, id);
    }
  }, [logLocal]);

  const setChaos = useCallback(async (next: Chaos) => {
    setChaosState(next); // optimistic
    try {
      setChaosState(await api.setChaos(next));
      logLocal(next.enabled ? 'chaos_on' : 'chaos_off',
        next.enabled ? `Chaos mode on: a random busy container is SIGKILLed every ${next.killEverySec}s` : 'Chaos mode off');
    } catch (err) {
      logLocal('failed', `Chaos toggle failed: ${(err as Error).message}`);
      setChaosState(await api.getChaos().catch(() => ({ ...next, enabled: !next.enabled })));
    }
  }, [logLocal]);

  return {
    job, workers, events, system, cluster, throttle, queues, chaos, connection, killRequested, pauseRequested, config,
    startSample, focusJob, killWorker, pauseWorker, setChaos, logLocal,
  };
}

export type Live = ReturnType<typeof useWildebeest>;

function withoutKey<T>(obj: Record<string, T>, key: string): Record<string, T> {
  const copy = { ...obj };
  delete copy[key];
  return copy;
}

function newestFirst(a: TaskEvent, b: TaskEvent): number {
  return Date.parse(b.at) - Date.parse(a.at) || Number(b.id) - Number(a.id);
}
