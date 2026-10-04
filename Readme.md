# Concurrent Writers & Conflict Management

## Problem

Concurrent inserts, mutations, and maintenance can conflict or silently lose updates. The challenge is to preserve every accepted operation while maximizing commit throughput.

## Hypothesis

Across disjoint-partition writes, overlapping-key mutations, and maintenance rewrites, P3 selective coordination will achieve higher median correct commit throughput than naive default writers while preserving all accepted operations with zero lost updates and zero incorrect final rows.

## Setup

Use one pinned Delta Lake or Iceberg runtime, catalog, and storage configuration that supports concurrent writes; environment pinning is pending. Run four workers (append, two mutation writers, and maintenance) across disjoint partitions, shared files, and overlapping keys, with maintenance on cold and active partitions. Recreate the same initial state and immutable operation intents for each paired comparison. Run ten fresh repetitions per scenario and policy with a common start barrier and reproducible scheduling seeds. Verify the frozen final snapshot against an independent oracle.

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
