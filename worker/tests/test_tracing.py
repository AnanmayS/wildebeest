"""Worker tracing: context extraction from the lease, the per-task process span and its children,
and the traceparent each report carries (also per item of a complete-batch)."""

import fakeredis
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ParentBased

from wildebeest_worker import tracing
from wildebeest_worker.runtime import Worker

from test_hotpath import HotCoordinator

TRACE_A = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_A = "00f067aa0ba902b7"


@pytest.fixture
def exporter():
    exp = InMemorySpanExporter()
    provider = TracerProvider(sampler=ParentBased(ALWAYS_ON))
    provider.add_span_processor(SimpleSpanProcessor(exp))
    tracing.use_provider(provider)
    yield exp
    tracing.disable()


def lease_context(sampled: bool = True) -> str:
    """What the coordinator puts in a lease: the context of its `lease` span for this attempt."""
    span = tracing._S.tracer.start_span("lease detect", context=trace.set_span_in_context(trace.INVALID_SPAN))
    span.end()
    tp = tracing.traceparent_of(span.get_span_context())
    return tp if sampled else tp[:-2] + "00"


class TracingCoordinator(HotCoordinator):
    """HotCoordinator that puts a traceparent in each lease and records request headers."""

    def __init__(self, contexts: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.contexts = contexts
        self.headers: list[tuple[str, dict | None]] = []

    def post(self, url, json, timeout, headers=None):
        self.headers.append((url.removeprefix("http://coord"), headers))
        resp = super().post(url, json, timeout)
        for lease in resp.json().get("leases", []):
            lease["traceparent"] = self.contexts.get(lease["taskId"])
        return resp


def traced_worker(contexts, handler=None, **coord_kwargs):
    coord = TracingCoordinator(contexts, **coord_kwargs)
    r = fakeredis.FakeRedis()
    coord.redis = r
    worker = Worker("detect", handler or (lambda lease: {"modelVersion": "v", "detections": []}), r,
                    "http://coord", http=coord, hostname="abc123", runtime="container")
    worker.register()
    return worker, coord, r


def by_name(exporter, name):
    return [s for s in exporter.get_finished_spans() if s.name == name]


# ------------------------------------------------------------------ context extraction


def test_extract_reads_the_lease_traceparent(exporter):
    ctx = tracing.extract(f"00-{TRACE_A}-{SPAN_A}-01")
    sc = trace.get_current_span(ctx).get_span_context()
    assert (f"{sc.trace_id:032x}", f"{sc.span_id:016x}") == (TRACE_A, SPAN_A)
    assert sc.is_remote and sc.trace_flags.sampled
    unsampled = trace.get_current_span(tracing.extract(f"00-{TRACE_A}-{SPAN_A}-00")).get_span_context()
    assert not unsampled.trace_flags.sampled
    # And back again, unchanged.
    assert tracing.traceparent_of(sc) == f"00-{TRACE_A}-{SPAN_A}-01"


@pytest.mark.parametrize("bad", [None, "", "garbage", 42, f"01-{TRACE_A}-{SPAN_A}-01", f"00-{'0' * 32}-{SPAN_A}-01",
                                 f"00-{TRACE_A}-{'0' * 16}-01", f"00-{TRACE_A.upper()}-{SPAN_A}-01"])
def test_extract_rejects_missing_or_malformed_contexts(exporter, bad):
    assert tracing.extract(bad) is None


def test_extract_is_a_no_op_with_tracing_off():
    assert not tracing.enabled()
    assert tracing.extract(f"00-{TRACE_A}-{SPAN_A}-01") is None


# ------------------------------------------------------------------ spans


def test_process_span_continues_the_lease_trace_and_the_report_carries_the_settle_context(exporter):
    tp = lease_context()
    worker, coord, r = traced_worker({"a": tp})
    r.rpush("queue:detect", "a")
    assert worker.run_once() == 1

    [process] = by_name(exporter, "process detect")
    assert process.kind == trace.SpanKind.CONSUMER
    assert f"{process.context.trace_id:032x}" == tp.split("-")[1]
    assert f"{process.parent.span_id:016x}" == tp.split("-")[2]  # under the coordinator's lease span
    assert process.attributes["messaging.message.id"] == "a"
    assert process.attributes["wildebeest.lease_epoch"] == 1
    [infer] = by_name(exporter, "infer")
    assert infer.parent.span_id == process.context.span_id

    [settle] = by_name(exporter, "settle detect")
    assert settle.kind == trace.SpanKind.CLIENT
    assert settle.parent.span_id == process.context.span_id
    assert settle.attributes["wildebeest.report.status"] == "ok"
    path, body = [(p, b) for p, b in coord.calls if p.endswith("/complete")][0]
    sent = tracing.traceparent_of(settle.context)
    assert body["traceparent"] == sent  # the coordinator's complete span nests under it
    assert dict(coord.headers)[path] == {"traceparent": sent}


def test_each_item_of_a_complete_batch_carries_its_own_trace(exporter):
    contexts = {"a": lease_context(), "b": lease_context()}
    worker, coord, r = traced_worker(contexts, max_batch=2, floor=2)
    r.rpush("queue:detect", "a", "b")
    assert worker.run_once() == 2

    [(_, body)] = [(p, b) for p, b in coord.calls if p == "/tasks/complete-batch"]
    for item in body["items"]:
        assert item["traceparent"].split("-")[1] == contexts[item["taskId"]].split("-")[1]
    settles = by_name(exporter, "settle detect")
    assert sorted(f"{s.context.trace_id:032x}" for s in settles) == sorted(c.split("-")[1] for c in contexts.values())
    assert all(s.attributes["messaging.batch.message_count"] == 2 for s in settles)
    assert dict(coord.headers)["/tasks/complete-batch"] is None  # no single trace for the request


def test_storage_phases_become_fetch_infer_upload_children(exporter):
    def handler(lease):
        with tracing.io_span("fetch", lease["imageKey"]):
            pass
        with tracing.io_span("upload", "crops/x.jpg"):
            pass
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = traced_worker({"a": lease_context()}, handler=handler)
    r.rpush("queue:detect", "a")
    worker.run_once()
    [process] = by_name(exporter, "process detect")
    fetch, infer, upload = (by_name(exporter, n)[0] for n in ("fetch", "infer", "upload"))
    assert {s.parent.span_id for s in (fetch, infer, upload)} == {process.context.span_id}
    assert fetch.end_time <= infer.start_time <= infer.end_time <= upload.start_time


def test_a_failed_task_marks_its_span_and_the_fail_report_is_its_child(exporter):
    def boom(lease):
        raise ValueError("bad model output")

    worker, coord, r = traced_worker({"a": lease_context()}, handler=boom)
    r.rpush("queue:detect", "a")
    worker.run_once()
    [process] = by_name(exporter, "process detect")
    assert process.status.status_code == trace.StatusCode.ERROR
    assert process.attributes["wildebeest.error.kind"] == "task"
    [settle] = by_name(exporter, "settle detect")
    assert settle.parent.span_id == process.context.span_id
    assert settle.attributes["wildebeest.report"] == "fail"
    [fail] = coord.paths("/fail")
    assert fail["traceparent"] == tracing.traceparent_of(settle.context)
    assert tracing._S.pending == {}  # nothing left waiting to be settled


def test_an_unsampled_lease_exports_nothing(exporter):
    worker, coord, r = traced_worker({"a": lease_context(sampled=False)})
    exporter.clear()
    r.rpush("queue:detect", "a")
    worker.run_once()
    assert exporter.get_finished_spans() == ()
    [(_, body)] = [(p, b) for p, b in coord.calls if p.endswith("/complete")]
    assert body["traceparent"].endswith("-00")  # the coordinator's spans stay unsampled too


def test_tracing_off_adds_nothing_to_requests():
    worker, coord, r = traced_worker({"a": f"00-{TRACE_A}-{SPAN_A}-01"})
    r.rpush("queue:detect", "a")
    worker.run_once()
    [(_, body)] = [(p, b) for p, b in coord.calls if p.endswith("/complete")]
    assert "traceparent" not in body
    assert all(h is None for _, h in coord.headers)
