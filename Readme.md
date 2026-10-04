# Concurrent Writers & Conflict Management

## Problem

Concurrent inserts, mutations, and maintenance can conflict or silently lose updates. The challenge is to preserve every accepted operation while maximizing commit throughput.

## Hypothesis

Across disjoint-partition writes, overlapping-key mutations, and maintenance rewrites, P3 selective coordination will achieve higher median correct commit throughput than naive default writers while preserving all accepted operations with zero lost updates and zero incorrect final rows.

## Setup

Use one pinned Delta Lake or Iceberg runtime, catalog, and storage configuration that supports concurrent writes; environment pinning is pending. Run four workers (append, two mutation writers, and maintenance) across disjoint partitions, shared files, and overlapping keys, with maintenance on cold and active partitions. Recreate the same initial state and immutable operation intents for each paired comparison. Run ten fresh repetitions per scenario and policy with a common start barrier and reproducible scheduling seeds. Verify the frozen final snapshot against an independent oracle.

## Scenarios

| ID | Workload |
| --- | --- |
| S1 — Disjoint partitions | A appends to p0; B updates p1; C updates p2; D compacts p3. |
| S2 — Disjoint keys in shared files | B updates keys 0–9 and C keys 10–19 in p0; D compacts p0. Distinct keys may still share physical files. |
| S3 — Overlapping keys | B and C both update keys 0–9 in p0; D compacts p3 to isolate mutation contention. |
| S4 — Overlap with active maintenance | Same overlapping updates as S3, while D compacts p0 to measure maintenance interference. |

## Policies

| ID | Write/conflict behavior |
| --- | --- |
| P0 — Naive baseline | Engine defaults; no explicit application retries, ownership, queue, or maintenance scheduling. |
| P1 — Narrow scopes | Add explicit partition predicates; keep P0 retry behavior. |
| P2 — Safe retries | P1 plus bounded exponential backoff with jitter, fresh transaction planning, and atomic idempotency. |
| P3 — Selective coordination | P2 plus queues/ownership only for conflicting keys or partitions; coordinate maintenance with the affected region. |
| P4 — Full serialization control | Run one transaction at a time across all workers, using P2 retry/idempotency settings. |

## Baseline

P0 is naive concurrent writing with the engine’s default commit behavior and no explicit application retry, ownership, queue, or maintenance scheduling strategy. Record the effective internal retry defaults.

## Method

Compare P0 with P3 using identical workloads. P3 uses narrow partition/key scopes, bounded retries with backoff and atomic idempotency, and coordinates only conflicting keys or partitions; run maintenance on unaffected or cold partitions where possible. Keep full serialization as a control to quantify its throughput cost. See the [detailed experiment plan](experiment-scenarios.md).

## Primary Metric

**Correct Commit Throughput** = unique successful durable commits / elapsed wall-clock seconds. Measure from barrier release until the final scheduled operation is resolved, including retries and outcome reconciliation. Report the median of ten repetitions for each scenario/policy only when every run passes the independent oracle: `LostUpdateCount = 0` and `WrongFinalRows = 0`. Any incorrect run fails and invalidates that primary result; duplicate retry acknowledgements do not count as additional commits.

## Final Result

Pending: no measured results are available. Environment pins and final results will be recorded after the experiment.

## Failure Case

Planned test, not yet observed: an ambiguous commit for overlapping counter increments is retried without atomic idempotency, applying an increment twice. The oracle should flag the incorrect final row. The [challenge brief, page 16](lakehouse-open-research-challenges-student-brief.pdf#page=16) defines the required correctness-gated objective.
