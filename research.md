# Research for concurrent writers and conflict management

Research date: 2026-10-04. Two research agents reviewed official Delta Lake and Apache Iceberg sources against [baseline.md](baseline.md). No experiments have been run; the strategies below are hypotheses to measure.

## R1: Delta Lake

- Delta uses optimistic concurrency: read a snapshot, stage files, then validate against intervening commits. Incompatible writes fail with conflict exceptions. Blind inserts do not conflict with other blind inserts or compaction, but UPDATE, DELETE, and MERGE can conflict with inserts, mutations, and compaction. Conflicts depend on files and read scope, so disjoint keys alone do not guarantee independence. Partitioning and explicit target partition literals in MERGE conditions can reduce unnecessary conflicts. Schema/metadata changes and protocol upgrades can conflict with writers. Streaming writers need separate checkpoints. [Concurrency control](https://docs.delta.io/concurrency-control/)
- MERGE source rows must be deduplicated when several source rows could update one target row. Delta 2.2 and later materializes sources to address nondeterminism across multiple passes. A foreachBatch MERGE must be idempotent because a restarted stream may replay a batch. [Updates and MERGE](https://docs.delta.io/delta-update/)
- Supported DataFrame writes can use txnAppId and increasing txnVersion to suppress duplicate writes; retries must reuse the identifiers with identical data. Independent concurrent jobs should not share one application ID with out-of-order versions because older versions are ignored. Commit userMetadata helps audit operations, and versionAsOf pins verification to a snapshot. Historical verification needs both log and data retention. Schema enforcement checks compatibility; identity columns disable concurrent transactions. [Batch writes, schema enforcement, idempotency, and time travel](https://docs.delta.io/delta-batch/)
- Bin-packing OPTIMIZE preserves logical rows, is idempotent, and supports partition predicates. Python/Scala optimize APIs require Delta 2.0 or later; auto-compaction requires 3.1 or later and runs synchronously after successful writes. Checkpoints compact log metadata for snapshot reconstruction. [Optimizations](https://docs.delta.io/optimizations-oss/)
- Storage configuration is part of experiment validity. The docs advise against concurrent-write tests on a local filesystem because atomic rename guarantees may be absent. HDFS supports concurrent transactional writes. Default S3 concurrency requires a single Spark driver; multiple drivers require the explicitly configured DynamoDB LogStore, consistently used by every writer. [Storage systems](https://docs.delta.io/delta-storage/)

Internal commit placement retries differ from retrying a failed logical MERGE or DELETE. Pin Delta and Spark versions and instrument application retries separately. Upstream implementation and settings can explain internal behavior, but must be checked against the chosen release: [OptimisticTransaction](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala), [DeltaSQLConf](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala).

## R2: Apache Iceberg

- Iceberg atomically replaces the current metadata pointer. Concurrent metadata commits may refresh and retry while reusing append manifests. A compaction retry is valid only while its original source files remain valid; if a competing operation removes a required source file, the rewrite must fail and subsequent work must replan from fresh state. [Reliability and concurrent writes](https://iceberg.apache.org/docs/nightly/reliability/#concurrent-write-operations)
- Nightly defaults list four commit retries, wait bounds of 100–60000 ms, and a total timeout of 1800000 ms. DELETE, UPDATE, and MERGE default to serializable isolation and copy-on-write. Commit status checks separately address uncertain outcomes after connection loss. Record actual effective settings for the pinned release. [Table behavior properties](https://iceberg.apache.org/docs/nightly/configuration/#table-behavior-properties)
- Snapshot expiration removes history and may reclaim files no retained snapshot needs. Orphan cleanup handles unreferenced files, including failed-job leftovers. Its retention interval must exceed the longest in-progress write; the documented default is three days. URI/path representation mismatches can incorrectly classify live files as orphaned. Data-file compaction combines small files; manifest rewrite reorganizes metadata for planning. [Maintenance](https://iceberg.apache.org/docs/nightly/maintenance/)
- Spark procedures support orphan-cleanup dry runs, expiration retention controls, partition-filtered data-file rewrites, and manifest rewrites. Nightly compaction defaults include partial-progress.enabled=false, max-concurrent-file-group-rewrites=5, and use-starting-sequence-number=true. Partial progress can produce multiple commits per job. Branch/tag references affect snapshot retention. [Spark procedures](https://iceberg.apache.org/docs/nightly/spark-procedures/)
- Spark supports INSERT, MERGE, UPDATE, and DELETE; row mutations require Iceberg Spark extensions. MERGE replaces affected files and requires at most one updating source record per target row. [Spark writes](https://iceberg.apache.org/docs/nightly/spark-writes/#merge-into)
- Delete application depends on partitions, file identity, and sequence numbers. Equality deletes generally apply to older data files: deleting a key does not automatically prevent a later reinsertion. Persistent privacy deletion requires an explicit application policy. [Scan planning specification](https://iceberg.apache.org/spec/#scan-planning)

R2 is a moving nightly reference. Pin the Iceberg release, Spark runtime, catalog, storage backend, table format, and properties, then confirm each API and default against that release.

## Proposed workload and comparisons

Use separate jobs/processes with a common start barrier, immutable generated inputs, fresh initial table state per run, and randomized scheduling delays.

| Writer | Workload | Correctness expectation |
| --- | --- | --- |
| Streaming insert | Small batches with predetermined unique event IDs | Each accepted logical event appears once |
| Historical backfill/mutation | Historical inserts and updates to seeded rows | Every accepted logical mutation is reflected |
| Maintenance | Partition-scoped compaction; manifest rewrite as an additional Iceberg scenario | Logical row multiset is unchanged |
| Optional privacy delete | Delete known seeded keys while mutations/compaction run | Final state follows a specified deletion-order policy |

Compare the same workload under these policies:

1. Baseline: concurrent writers with pinned default behavior and no explicit conflict strategy.
2. Narrow target predicates and partition scopes.
3. Narrow scopes plus bounded application retries with exponential backoff, jitter, outcome reconciliation, and idempotency.
4. Partition ownership or selective queues for conflicting operations; schedule maintenance on colder partitions.
5. Full serialization as a throughput reference.

| Scenario | Purpose |
| --- | --- |
| Disjoint partitions | Measure throughput when write scopes are independent |
| Different keys within the same partition/files | Reveal physical conflict granularity |
| Overlapping seeded keys | Exercise genuine mutation conflicts |
| Compaction on cold versus actively mutated partitions | Measure maintenance interference |
| Privacy deletes racing with mutations/compaction | Check explicit ordering and reinsertion semantics |

Start with at least 10 repetitions per scenario and policy. Increase repetitions when failures or high variance occur. This repetition count is a proposed experiment setting, not a documentation requirement.

## Independent final-state oracle

Generate initial rows and operation intents outside the table implementation. Compute expected results with independent Python logic; do not derive expected rows from the final table itself.

- Unique-ID appends and updates to different per-writer fields provide an order-independent initial workload. Seeded counter increments can also commute, provided the application implements each accepted increment exactly once rather than replaying stale absolute values.
- Compaction and manifest rewrite must preserve the full logical row multiset.
- For order-sensitive overwrites and deletes, use a controlled ordering or replay accepted logical operations in verified table-version/snapshot commit order. Submission order is insufficient.
- Reconcile uncertain outcomes before retrying and before constructing the accepted-operation set. A success acknowledgement alone does not establish exactly-once application.
- Compare full rows, missing rows, unexpected rows, duplicate IDs, and mismatched values. Check that every accepted logical operation contributes its required effect.
- Freeze the final version/snapshot for verification and retain the files needed to read it. Run expiration/cleanup separately after verification with appropriate retention safeguards.
- An external audit table alone does not prove atomicity across tables. Tie operation evidence to the data commit and reconcile ambiguous outcomes.
- Report permanently aborted and unfinished planned operations separately so abandoning work cannot artificially improve correctness or throughput.

## Measurements and decision rule

Primary objective: maximize successful durable commits per wall-clock second, subject to LostUpdateCount = 0 and WrongFinalRows = 0. Any violating run fails the primary metric; report failures alongside throughput rather than averaging them away.

Count durable commits once, excluding duplicate retry acknowledgements. Report data commits, maintenance commits, and completed logical operations separately because one job may create multiple commits and one commit may contain many operations.

Record conflict classifications/rates, metadata commit retries, complete-operation retries, aborted operations, unknown outcomes, p95 attempt latency, and p95 end-to-end latency including backoff and queueing. Define write amplification as physical bytes written, including failed attempts and maintenance, divided by successful logical payload bytes; report raw bytes where the denominator is zero.

The candidate strategy is narrow conflict scopes, safe bounded retries, and selective coordination for hotspots. Select it only if repeated measurements show zero correctness violations and better correct commit throughput than the default baseline and the serialization reference.
