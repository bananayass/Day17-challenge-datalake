# 11. Concurrent Writers & Conflict Management

ACID does not mean every concurrent write should succeed.

## Production pain point

A real table may receive streaming inserts, historical backfills, privacy deletes, and compaction at the same time. Correct systems should never silently lose updates, but excessive conflicts and retries can destroy throughput.

## Problem statement

Create a concurrent-write workload and design a strategy that preserves correctness while maximizing commit throughput.

## Minimum constraints

- Run at least three writer types or access patterns (for example: disjoint partitions, overlapping keys, maintenance rewrite).
- Include a known-correct final-state oracle so silent lost updates can be detected.
- Run each scenario multiple times; concurrency bugs are nondeterministic.
- Do not hide conflicts by serializing all writers unless you quantify the throughput cost.

## Open research space

Directions: optimistic concurrency, partition/key-level conflict avoidance, retries with backoff, idempotent writes, write-audit tables, queue/serialization for only conflicting operations, workload partitioning.

## Baseline

Naive concurrent writers with default retry behavior and no explicit conflict strategy.

## Minimum experiment

Create concurrent processes/threads/jobs that operate on disjoint and overlapping regions. Verify the final table against an independently computed oracle.

## Primary metric: Correct Commit Throughput

**Formula / rule:** Primary objective: maximize `successful_commits / second` subject to `LostUpdateCount = 0` and `WrongFinalRows = 0`. Any run violating correctness fails the primary metric.

Direction: Higher throughput is better only after correctness constraints are satisfied.

**Secondary metrics:** Conflict rate; retry count; p95 commit latency; aborted transactions; write amplification; throughput by scenario.

## What a strong result looks like

- Zero silent lost updates across repeated runs.
- High throughput on disjoint writes while safely rejecting/retrying true conflicts.
- Explains which operations can commute and which must conflict.
- Shows p95 latency and retry behavior, not only average throughput.

*AICB - Data Lakehouse Open Research Challenge | Student Brief | 17*

## Common pitfalls / invalid evidence

- Measuring only job success codes without verifying final data.
- Calling serialization the "best" solution without throughput comparison.
- Testing only non-overlapping writes.
- Ignoring idempotency when retrying failed commits.
