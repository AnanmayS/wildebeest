"""OpenTelemetry tracing for the worker (docs/decisions/o-observability.md).

Per task, under the lease's `traceparent` (the coordinator's `lease {stage}` span for this attempt,
itself a child of the task's PRODUCER span, so one trace per image across every attempt):

    process {stage}   CONSUMER  the handler run (one per task, also for batch-claimed leases)
    ├─ fetch          CLIENT    object-storage read (with prefetch: only the part we waited for)
    ├─ infer                    handler time between the last fetch and the first upload
    ├─ upload         CLIENT    crop write
    └─ settle {stage} CLIENT    the complete / fail / release request; its traceparent travels in
                                the report body (and header) so the coordinator's `complete` span
                                nests under it, one per task even inside a complete-batch

Off unless OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is set and the OTel
packages are installed (requirements-otel.txt). Off means every function below returns at its
first line and the opentelemetry packages are never imported. OTEL_TRACES_SAMPLER /
OTEL_TRACES_SAMPLER_ARG pick the sampler; the default, parentbased_always_on, follows the
coordinator's decision carried in the traceparent flags.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import socket
import threading
import time
from collections import OrderedDict
from typing import Any, Iterator

log = logging.getLogger("wildebeest.tracing")

_TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.enabled = False
        self.setup_done = False
        self.tracer: Any = None
        self.provider: Any = None
        # Process-span contexts of finished tasks waiting in the report buffer, by (taskId, epoch).
        self.pending: OrderedDict[tuple[str, int], Any] = OrderedDict()


_S = _State()
_local = threading.local()  # per thread: phase marks of the task running on it
MAX_PENDING = 10_000


def enabled() -> bool:
    return _S.enabled


def requested(env: os._Environ | dict = os.environ) -> bool:
    if env.get("OTEL_SDK_DISABLED", "").lower() == "true":
        return False
    return bool(env.get("OTEL_EXPORTER_OTLP_ENDPOINT") or env.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))


def setup(stage: str | None = None) -> bool:
    """Installs the SDK once per process if tracing is requested. Safe to call from every Worker."""
    with _S.lock:
        if _S.setup_done:
            return _S.enabled
        _S.setup_done = True
        if not requested():
            return False
        try:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
        except ImportError as e:
            log.warning("tracing requested but OpenTelemetry is not installed (%s); continuing without it", e)
            return False
        attrs = {
            "service.name": os.environ.get("OTEL_SERVICE_NAME") or "wildebeest-worker",
            "service.namespace": "wildebeest",
            "service.instance.id": socket.gethostname(),
        }
        if stage:
            attrs["wildebeest.stage"] = stage
        provider = TracerProvider(resource=Resource.create(attrs))  # sampler from OTEL_TRACES_SAMPLER
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
        _install(provider)
    log.info("tracing on: OTLP %s, sampler %s", os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
             or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"), os.environ.get("OTEL_TRACES_SAMPLER", "parentbased_always_on"))
    return True


def _install(provider: Any) -> None:
    _S.provider = provider
    _S.tracer = provider.get_tracer("wildebeest-worker")
    _S.enabled = True


def use_provider(provider: Any) -> None:
    """Tests: trace into this provider (e.g. with an InMemorySpanExporter)."""
    with _S.lock:
        _S.setup_done = True
        _install(provider)
        _S.pending.clear()


def disable() -> None:
    """Tests: back to off."""
    with _S.lock:
        _S.enabled = False
        _S.tracer = None
        _S.pending.clear()


# ------------------------------------------------------------------------ context


def extract(traceparent: Any):
    """W3C traceparent (from a lease or a report) -> OTel Context, or None if absent/invalid."""
    if not _S.enabled or not isinstance(traceparent, str):
        return None
    m = _TRACEPARENT.match(traceparent)
    if not m or int(m.group(1), 16) == 0 or int(m.group(2), 16) == 0:
        return None
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    return TraceContextTextMapPropagator().extract({"traceparent": traceparent})


def traceparent_of(span_context: Any) -> str:
    return f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-{int(span_context.trace_flags):02x}"


def _messaging(op: str, stage: str, lease: dict, worker_id: str | None) -> dict:
    return {
        "messaging.system": "wildebeest",
        "messaging.operation.name": op,
        "messaging.operation.type": op,
        "messaging.destination.name": stage,
        "messaging.message.id": lease.get("taskId", ""),
        "messaging.client.id": worker_id or "",
        "wildebeest.task_id": lease.get("taskId", ""),
        "wildebeest.stage": stage,
        "wildebeest.lease_epoch": int(lease.get("leaseEpoch") or 0),
        "wildebeest.worker_id": worker_id or "",
    }


# ------------------------------------------------------------------------ spans


@contextlib.contextmanager
def process(lease: dict, stage: str, worker_id: str | None, claim_ms: float = 0.0) -> Iterator[Any]:
    """CONSUMER `process {stage}` span around one task; active while the handler runs."""
    if not _S.enabled:
        yield None
        return
    from opentelemetry import context as otel_context, trace

    attrs = _messaging("process", stage, lease, worker_id)
    attrs["wildebeest.image_key"] = lease.get("imageKey") or ""
    attrs["wildebeest.claim_ms"] = round(claim_ms, 1)
    attrs["wildebeest.speculative"] = bool(lease.get("speculative"))
    span = _S.tracer.start_span(f"process {stage}", context=extract(lease.get("traceparent")),
                                kind=trace.SpanKind.CONSUMER, attributes=attrs)
    token = otel_context.attach(trace.set_span_in_context(span))
    marks = _local.marks = {"start": time.time_ns(), "fetch_end": None, "upload_start": None}
    try:
        yield span
    finally:
        _local.marks = None
        otel_context.detach(token)
        end = time.time_ns()
        if span.is_recording():
            _infer_span(span, marks, end)
        span.end(end_time=end)
        if marks.get("settled"):
            return  # reported (fail/release) inside the span: nothing left to settle
        with _S.lock:
            _S.pending[(lease.get("taskId", ""), int(lease.get("leaseEpoch") or 0))] = span.get_span_context()
            while len(_S.pending) > MAX_PENDING:
                _S.pending.popitem(last=False)


def _infer_span(parent: Any, marks: dict, end: int) -> None:
    """`infer` = handler time after the last storage read and before the first write."""
    from opentelemetry import trace

    start = marks["fetch_end"] or marks["start"]
    stop = marks["upload_start"] or end
    if stop > start:
        span = _S.tracer.start_span("infer", context=trace.set_span_in_context(parent), start_time=start)
        span.end(end_time=stop)


@contextlib.contextmanager
def io_span(name: str, key: str, **attrs: Any) -> Iterator[None]:
    """`fetch` / `upload`: a CLIENT span around an object-storage call inside a process span."""
    marks = getattr(_local, "marks", None) if _S.enabled else None
    if marks is None:
        yield
        return
    from opentelemetry import trace

    if name == "upload" and marks["upload_start"] is None:
        marks["upload_start"] = time.time_ns()
    span = _S.tracer.start_span(name, kind=trace.SpanKind.CLIENT,
                                attributes={"wildebeest.object_key": key, "rpc.system": "s3", **attrs})
    try:
        yield
    except BaseException as e:
        span.record_exception(e)
        span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
        raise
    finally:
        span.end()
        if name == "fetch":
            marks["fetch_end"] = time.time_ns()


def record_error(e: BaseException, kind: str) -> None:
    """Marks the running task's process span failed (infra | fatal | task)."""
    if not _S.enabled:
        return
    from opentelemetry import trace

    span = trace.get_current_span()
    if span.is_recording():
        span.record_exception(e)
        span.set_attribute("wildebeest.error.kind", kind)
        span.set_status(trace.Status(trace.StatusCode.ERROR, f"{kind}: {e}"))


class Settle:
    """The `settle {stage}` spans of one report request (complete, complete-batch, fail, release)."""

    def __init__(self, spans: list) -> None:
        self.spans = spans

    def headers(self) -> dict:
        """traceparent header for a single-task report, so the coordinator's http span joins the trace."""
        if len(self.spans) == 1 and self.spans[0] is not None:
            return {"traceparent": traceparent_of(self.spans[0].get_span_context())}
        return {}

    def done(self, statuses: list[str] | None = None, http_status: int | None = None,
             error: BaseException | None = None) -> None:
        from opentelemetry import trace

        for i, span in enumerate(self.spans):
            if span is None:
                continue
            if http_status is not None:
                span.set_attribute("http.response.status_code", http_status)
            status = statuses[i] if statuses and i < len(statuses) else None
            if status:
                span.set_attribute("wildebeest.report.status", status)
                if status != "ok":
                    span.set_status(trace.Status(trace.StatusCode.ERROR, "STALE_LEASE" if status == "stale" else status))
            if error is not None:
                span.record_exception(error)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(error)))
            span.end()


_NOOP = Settle([])


def settle(action: str, stage: str, worker_id: str | None, items: list[dict], batch_size: int = 1) -> Settle:
    """Opens one CLIENT span per reported task and writes its traceparent into the item (the
    request body), parented on that task's process span. For fail/release, which are sent while the
    process span is still active, the active span is the parent."""
    if not _S.enabled:
        return _NOOP
    from opentelemetry import trace

    spans = []
    for item in items:
        key = (item.get("taskId", ""), int(item.get("leaseEpoch") or 0))
        with _S.lock:
            parent_sc = _S.pending.pop(key, None)
        if parent_sc is not None:
            ctx = trace.set_span_in_context(trace.NonRecordingSpan(parent_sc))
        elif trace.get_current_span().get_span_context().is_valid:
            ctx = None  # the active process span
            marks = getattr(_local, "marks", None)
            if marks is not None:
                marks["settled"] = True
        else:
            spans.append(None)
            continue
        attrs = _messaging("settle", stage, item, worker_id)
        attrs["wildebeest.report"] = action
        attrs["messaging.batch.message_count"] = batch_size
        span = _S.tracer.start_span(f"settle {stage}", context=ctx, kind=trace.SpanKind.CLIENT, attributes=attrs)
        item["traceparent"] = traceparent_of(span.get_span_context())
        spans.append(span)
    return Settle(spans)
