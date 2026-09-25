// OpenTelemetry SDK bootstrap for the coordinator (docs/decisions/o-observability.md).
//
// Tracing is on only when OTEL_EXPORTER_OTLP_ENDPOINT (or ..._TRACES_ENDPOINT) is set. With it
// unset nothing here runs: no SDK, no instrumentation hooks, and every trace hook in otel.ts
// returns on its first line, so tracing off costs nothing.
//
// This module must not import application code (db, redis, ...): otel-preload.ts loads it
// before the app, so that pg / ioredis / http are patched when the app first imports them.
//
// Standard OTel env vars apply: OTEL_TRACES_SAMPLER (+ _ARG) picks the sampler (e.g.
// parentbased_traceidratio with 0.1 = keep 10% of image traces), OTEL_BSP_* tune the batch span
// processor, OTEL_SERVICE_NAME names the service.

import os from "node:os";
import type { SpanExporter, Sampler } from "@opentelemetry/sdk-trace-node";

let enabled = false;
let shutdownFn: (() => Promise<void>) | null = null;

/** True once setupTracing() ran (and not switched off since). Checked by every trace hook. */
export const tracingEnabled = () => enabled;

/** Test hook: keep an SDK installed but make every trace hook a no-op again. */
export function setTracingEnabled(on: boolean) {
  enabled = on;
}

/** Whether the environment asks for tracing (an OTLP endpoint is configured). */
export function tracingRequested(env: NodeJS.ProcessEnv = process.env): boolean {
  if (env.OTEL_SDK_DISABLED === "true") return false;
  return Boolean(env.OTEL_EXPORTER_OTLP_ENDPOINT || env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT);
}

export interface TracingOptions {
  /** Default: OTLP/HTTP protobuf to OTEL_EXPORTER_OTLP_ENDPOINT. Tests pass an InMemorySpanExporter. */
  exporter?: SpanExporter;
  /** Export each span as it ends (tests) instead of batching (default). */
  simple?: boolean;
  /** Default: from OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG (parentbased_always_on if unset). */
  sampler?: Sampler;
  /** Patch http, pg and ioredis (needs the ESM loader hook from otel-preload.ts). */
  instrument?: boolean;
}

let forcedSpanId: string | null = null;

/**
 * Runs fn with the next span ID fixed to `spanId` (16 hex). otel.ts derives the IDs of the
 * spans another replica must be able to close or parent under (a task's PRODUCER span and each
 * attempt's lease span) from the task ID and epoch, so no replica needs another's memory.
 */
export function withSpanId<T>(spanId: string, fn: () => T): T {
  forcedSpanId = spanId;
  try {
    return fn();
  } finally {
    forcedSpanId = null;
  }
}

export async function setupTracing(opts: TracingOptions = {}): Promise<void> {
  const { NodeTracerProvider, BatchSpanProcessor, SimpleSpanProcessor, RandomIdGenerator } = await import(
    "@opentelemetry/sdk-trace-node"
  );
  const random = new RandomIdGenerator();
  const idGenerator = {
    generateTraceId: () => random.generateTraceId(),
    generateSpanId: () => forcedSpanId ?? random.generateSpanId(),
  };
  const { resourceFromAttributes } = await import("@opentelemetry/resources");

  const exporter =
    opts.exporter ?? new (await import("@opentelemetry/exporter-trace-otlp-proto")).OTLPTraceExporter();
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      "service.name": process.env.OTEL_SERVICE_NAME || "wildebeest-coordinator",
      "service.namespace": "wildebeest",
      "service.instance.id": process.env.COORDINATOR_ID || os.hostname(),
    }),
    ...(opts.sampler ? { sampler: opts.sampler } : {}),
    idGenerator,
    spanProcessors: [opts.simple ? new SimpleSpanProcessor(exporter) : new BatchSpanProcessor(exporter)],
  });
  provider.register(); // global tracer provider, W3C propagator, AsyncLocalStorage context manager

  // OTEL_NODE_DISABLED_INSTRUMENTATIONS=http,pg,ioredis (the auto-instrumentations-node convention)
  // switches individual libraries off; the per-image task spans stay.
  const disabled = new Set((process.env.OTEL_NODE_DISABLED_INSTRUMENTATIONS ?? "").split(",").map((s) => s.trim()));
  if (opts.instrument && !["http", "pg", "ioredis"].every((n) => disabled.has(n))) {
    const { registerInstrumentations } = await import("@opentelemetry/instrumentation");
    const { HttpInstrumentation } = await import("@opentelemetry/instrumentation-http");
    const { PgInstrumentation } = await import("@opentelemetry/instrumentation-pg");
    const { IORedisInstrumentation } = await import("@opentelemetry/instrumentation-ioredis");
    const instrumentations = [
      !disabled.has("http") &&
        new HttpInstrumentation({
          // Selective on purpose (Platformatic measured full auto-instrumentation at -80% on a
          // hello-world server). An incoming request is traced only when the caller propagated a
          // trace context: a worker reporting on one image (complete / fail / release). Heartbeats,
          // claims, batched completes and dashboard polls start no traces of their own; the task
          // spans in otel.ts cover them per image.
          ignoreIncomingRequestHook: (req) => !req.headers.traceparent,
          // No express instrumentation: name server spans by route template ("POST /tasks/:id/complete").
          requestHook: (span, req) => {
            if ("setHeader" in req) return; // outgoing
            const route = (req.url ?? "").split("?")[0].replace(/\/(tasks|workers)\/[^/]+\/(?=[a-z-]+$)/, "/$1/:id/");
            span.updateName(`${req.method} ${route}`);
            span.setAttribute("http.route", route);
          },
          // Outgoing calls (S3, the Docker socket) only inside a trace we are already in.
          requireParentforOutgoingSpans: true,
        }),
      // Statements and Redis commands only inside a traced request (no root span per query);
      // pool connect spans add nothing a trace reader needs.
      !disabled.has("pg") && new PgInstrumentation({ requireParentSpan: true, ignoreConnectSpans: true }),
      !disabled.has("ioredis") && new IORedisInstrumentation({ requireParentSpan: true }),
    ];
    registerInstrumentations({ instrumentations: instrumentations.filter((i) => i !== false) });
  }

  shutdownFn = () => provider.shutdown();
  enabled = true;
}

/** Flushes and stops the SDK (graceful shutdown, tests); gives up after `timeoutMs`. */
export async function shutdownTracing(timeoutMs = 3000): Promise<void> {
  enabled = false;
  const fn = shutdownFn;
  shutdownFn = null;
  if (!fn) return;
  let timer: NodeJS.Timeout | undefined;
  await Promise.race([
    fn().catch(() => {}),
    new Promise<void>((resolve) => (timer = setTimeout(resolve, timeoutMs).unref())),
  ]);
  clearTimeout(timer);
}
