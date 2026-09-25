# Fault matrix

Jepsen-lite: seeded fault schedules against a running fake-backend job, then an offline check of the coordinator's own history (tests/invariants/checker.py). Per-run tables are in each directory's results.md.

| Code | Runs | Faults injected | Violations | Violations by invariant | Fenced late results | Failed images | Re-executions |
|---|---|---|---|---|---|---|---|
| [before](before/results.md) | 12 | 60 | **4** | I7 job_finishes: 4 | 52 | 71 | 203 |
