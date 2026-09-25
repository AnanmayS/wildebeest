"""Generates dashboards/wildebeest.json (the provisioned Grafana dashboard). Run after editing:
    python observability/grafana/gen_dashboard.py
"""
import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
TEMPO = {"type": "tempo", "uid": "tempo"}
panels = []
pid = 0


def nid():
    global pid
    pid += 1
    return pid


def row(title, y):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})


def ts(title, targets, x, y, w=8, h=8, unit="short", desc="", stack=False, min0=True):
    p = {
        "type": "timeseries", "title": title, "id": nid(), "datasource": PROM, "description": desc,
        # The bundled Prometheus datasource declares a 60 s scrape interval; ours is 2 s.
        "interval": "5s",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "min": 0 if min0 else None,
                                     "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never",
                                                "stacking": {"mode": "normal" if stack else "none"}}},
                        "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [{"datasource": PROM, "refId": chr(65 + i), "expr": e, "legendFormat": l, "range": True}
                    for i, (e, l) in enumerate(targets)],
    }
    if not min0:
        del p["fieldConfig"]["defaults"]["min"]
    panels.append(p)


def stat(title, expr, x, y, w=4, h=5, unit="short", desc="", thresholds=None, decimals=None):
    steps = thresholds or [{"color": "green", "value": None}]
    p = {
        "type": "stat", "title": title, "id": nid(), "datasource": PROM, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps},
                                     "color": {"mode": "thresholds"}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "auto", "justifyMode": "auto"},
        "targets": [{"datasource": PROM, "refId": "A", "expr": expr, "instant": True, "range": False}],
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    panels.append(p)


def traces(title, query, x, y, w=12, h=9, desc=""):
    panels.append({
        "type": "table", "title": title, "id": nid(), "datasource": TEMPO, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "options": {"showHeader": True, "cellHeight": "sm"},
        "fieldConfig": {"defaults": {}, "overrides": []},
        "targets": [{"datasource": TEMPO, "refId": "A", "queryType": "traceql", "query": query,
                     "limit": 20, "tableType": "traces"}],
    })


RED = "red"
y = 0
row("Throughput and errors (RED, per stage)", y); y += 1
ts("Completions / s", [
    ('sum by (stage) (rate(wildebeest_tasks_completed_total[30s]))', "{{stage}}"),
], 0, y, desc="Accepted completions per second (fenced-off writes excluded).", unit="ops")
ts("Errors / s", [
    ('sum by (stage, final) (rate(wildebeest_task_failures_total[30s]))', "task error {{stage}} final={{final}}"),
    ('sum by (stage, reason) (rate(wildebeest_lease_expirations_total[30s]))', "lease lost {{stage}} ({{reason}})"),
    ('sum(rate(wildebeest_stale_write_rejections_total[30s]))', "stale write fenced off"),
], 8, y, unit="ops", desc="Worker-reported task errors (/fail), leases lost to dead workers or expiry, and late writes fenced off by the lease epoch.")
ts("Handler latency p95 (worker protocol)", [
    ('histogram_quantile(0.95, sum by (le, route) (rate(wildebeest_http_request_duration_seconds_bucket[30s])))', "{{route}}"),
], 16, y, unit="s", desc="Coordinator handling time of claim / complete / heartbeat requests (postgres-mode /tasks/claim includes its long-poll wait).")
y += 8

row("Latency (per stage)", y); y += 1
ts("Service time p50 / p95", [
    ('histogram_quantile(0.5, sum by (le, stage) (rate(wildebeest_task_service_seconds_bucket[30s])))', "p50 {{stage}}"),
    ('histogram_quantile(0.95, sum by (le, stage) (rate(wildebeest_task_service_seconds_bucket[30s])))', "p95 {{stage}}"),
], 0, y, w=12, unit="s", desc="Claimed → completion handled.")
ts("Queue wait p50 / p95", [
    ('histogram_quantile(0.5, sum by (le, stage) (rate(wildebeest_task_queue_wait_seconds_bucket[30s])))', "p50 {{stage}}"),
    ('histogram_quantile(0.95, sum by (le, stage) (rate(wildebeest_task_queue_wait_seconds_bucket[30s])))', "p95 {{stage}}"),
], 12, y, w=12, unit="s", desc="Ready → claimed (backlog held in Postgres + time in the ready queue).")
y += 8

row("Saturation", y); y += 1
ts("Queue depth", [
    ('max by (stage) (wildebeest_queue_depth)', "ready {{stage}}"),
], 0, y, desc="Ready tasks per stage: Redis list length (hybrid) or claimable PENDING rows (postgres).")
ts("Leases in flight", [
    ('max by (stage) (wildebeest_leases_in_flight)', "{{stage}}"),
], 8, y)
ts("Workers", [
    ('max by (stage, status) (wildebeest_workers{status!="STOPPED"})', "{{stage}} {{status}}"),
    ('max(wildebeest_throttled)', "throttled (backpressure)"),
], 16, y, desc="Workers seen in the last 10 minutes by status, and the backpressure flag.")
y += 8

row("Fault tolerance", y); y += 1
stat("Stale writes fenced off", 'sum(increase(wildebeest_stale_write_rejections_total[$__range]))', 0, y,
     desc="Late results from workers whose lease was taken over, rejected by the epoch check (in the time range).",
     decimals=0)
stat("Worker recoveries", 'sum(increase(wildebeest_recoveries_total[$__range]))', 4, y, decimals=0,
     desc="Deaths detected (Docker event or heartbeat timeout) and handled in the time range.")
stat("Recovery p50", 'histogram_quantile(0.5, sum by (le) (increase(wildebeest_recovery_seconds_bucket[$__range])))', 8, y,
     unit="s", desc="Best known moment of death → every lost task claimed again (bucketed).")
stat("Recovery p95", 'histogram_quantile(0.95, sum by (le) (increase(wildebeest_recovery_seconds_bucket[$__range])))', 12, y,
     unit="s")
stat("Invariant violations", 'sum(max by (kind) (wildebeest_invariant_violations))', 16, y, decimals=0,
     thresholds=[{"color": "green", "value": None}, {"color": RED, "value": 1}],
     desc="Live check: duplicate results, stuck leases, lost images. Must stay 0.")
stat("Cache hits", 'sum(increase(wildebeest_cache_hits_total[$__range]))', 20, y, decimals=0,
     desc="Images answered from the content-hash cache at job creation (time range).")
y += 5

row("Traces (Tempo)", y); y += 1
traces("Reclaimed tasks: traces with a requeue", '{ name =~ "requeue (detect|classify)" }', 0, y,
       desc="Each row is one image whose attempt was lost (worker died or lease expired). Open a trace: the lost attempt's lease span ends in error, the requeue follows, and the next attempt runs with a higher epoch.")
traces("Fenced-off writes", '{ name =~ "complete (detect|classify)" && span.wildebeest.write.accepted = false }', 12, y,
       desc="Late results rejected by the lease epoch (zombie workers).")
y += 9
traces("Recent image traces", '{ name =~ "create (detect|classify)" && kind = producer }', 0, y, w=24, h=8,
       desc="Every traced image starts with its PRODUCER span at job creation.")
y += 8

row("Workers (from heartbeats)", y); y += 1
ts("Worker RSS", [('max by (worker) (wildebeest_worker_rss_bytes)', "{{worker}}")], 0, y, w=12, unit="bytes")
ts("Worker claim window", [('max by (worker) (wildebeest_worker_claim_batch)', "{{worker}}")], 12, y, w=12,
   desc="Leases each worker aims to hold (≈ RTT ÷ service time, at least 2 with prefetch).")
y += 8

dash = {
    "uid": "wildebeest-overview",
    "title": "Wildebeest",
    "description": "RED per stage, saturation, fault tolerance and per-image traces. docs/decisions/o-observability.md",
    "tags": ["wildebeest"],
    "timezone": "browser",
    "editable": True,
    "graphTooltip": 1,
    "refresh": "5s",
    "schemaVersion": 39,
    "version": 1,
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {},
    "templating": {"list": []},
    "annotations": {"list": []},
    "panels": panels,
}
out = Path(__file__).resolve().parent / "dashboards" / "wildebeest.json"
with open(out, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print(f"{len(panels)} panels -> {out}")
