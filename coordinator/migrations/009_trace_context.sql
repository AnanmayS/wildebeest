-- Observability (docs/decisions/o-observability.md): the W3C trace context of the task's
-- OpenTelemetry PRODUCER span ("create detect" / "create classify"), e.g.
-- 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01.
--
-- It lives on the row, not in Redis or coordinator memory, so a task keeps its trace across
-- attempts: a SIGKILLed worker's lease, the requeue and the next worker's lease all hang off the
-- same trace. NULL when the task was created with tracing off (OTEL_EXPORTER_OTLP_ENDPOINT unset);
-- such a task is simply not traced. Nullable, no default: adding it rewrites nothing.
alter table tasks add column if not exists traceparent text;
