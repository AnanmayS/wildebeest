import type { Benchmarks, Chaos, Cluster, Config, GalleryImage, JobSummary, SystemSnapshot, TaskEvent, Worker } from './types';

// Every call goes through the same-origin /api prefix (nginx in prod, Vite proxy in dev).
export class ApiError extends Error {
  constructor(message: string, readonly status: number) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new ApiError(body.error ?? `${res.status} ${res.statusText}`, res.status);
  }
  return res.json() as Promise<T>;
}

const postJson = <T>(path: string, body: unknown) =>
  request<T>(path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });

export const api = {
  listJobs: () => request<{ jobs: JobSummary[] }>('/jobs').then((r) => r.jobs),
  getJob: (id: string) => request<JobSummary>(`/jobs/${id}`),
  startSample: (size: number, opts: { fresh?: boolean; random?: boolean } = {}) =>
    postJson<{ jobId: string }>('/jobs/sample', { size, ...opts }),
  listWorkers: () => request<{ workers: Worker[] }>('/workers').then((r) => r.workers),
  killWorker: (id: string) => postJson<{ ok: boolean }>(`/workers/${id}/kill`, {}),
  /** v2: `docker pause` for ms, then unpause. 409 NOT_A_CONTAINER for native workers. */
  pauseWorker: (id: string, ms: number) => postJson<{ ok: boolean }>(`/workers/${id}/pause`, { ms }),
  /** v2: the dashboard's main data. 404 on older coordinators. */
  getSystem: () => request<SystemSnapshot>('/system'),
  /** v2: benchmarks/summary.json. 404 until the benchmark harness has run. */
  getBenchmarks: () => request<Benchmarks>('/benchmarks'),
  /** HA: who leads, and which replicas are alive. 404 on a single-coordinator build. */
  getCluster: () => request<Cluster>('/cluster'),
  getChaos: () => request<Chaos>('/chaos'),
  setChaos: (chaos: Chaos) => postJson<Chaos>('/chaos', chaos),
  getMetrics: () => request<{ throttled?: boolean; queues?: { detect?: number; classify?: number } }>('/metrics'),
  getConfig: () => request<Partial<Config>>('/config'),
  /** Recent events, newest first — seeds the event log after a reload or reconnect. */
  listEvents: (limit = 200) => request<{ events: TaskEvent[] }>(`/events?limit=${limit}`).then((r) => r.events),

  listImages: (jobId: string, q: { category?: string; species?: string; page: number; pageSize: number }) => {
    const params = new URLSearchParams({ page: String(q.page), pageSize: String(q.pageSize) });
    if (q.category) params.set('category', q.category);
    if (q.species) params.set('species', q.species);
    return request<{ images: GalleryImage[]; total: number; page: number }>(`/jobs/${jobId}/images?${params}`);
  },
};

/** Multipart upload (field `files`). Uses XHR so we can show upload progress for big batches. */
export function uploadPhotos(files: File[], onProgress: (fraction: number) => void): Promise<{ jobId: string }> {
  return new Promise((resolve, reject) => {
    const form = new FormData();
    for (const f of files) form.append('files', f, f.name);
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/jobs');
    xhr.upload.onprogress = (e) => e.lengthComputable && onProgress(e.loaded / e.total);
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve(JSON.parse(xhr.responseText));
      else reject(new Error(`Upload failed (${xhr.status})`));
    };
    xhr.onerror = () => reject(new Error('Upload failed (network)'));
    xhr.send(form);
  });
}
