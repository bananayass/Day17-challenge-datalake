# Experiment scenarios: concurrent writers and conflict management

This draft operationalizes the minimum constraints in [baseline.md](baseline.md), using the findings in [research.md](research.md). It is an experiment plan; no results are claimed.

## Research question

How much correct commit throughput can concurrent inserts, overlapping mutations, and maintenance achieve under default behavior? Can narrower write scopes, safe retries, and selective coordination improve throughput while preserving every accepted logical operation?

Run the first version locally with Polars and Parquet. The scripts include a small optimistic commit simulator because Polars does not provide table transactions. Results describe this simulator rather than Delta Lake or Iceberg behavior.

## Minimum-constraint coverage

| Baseline constraint | How this plan satisfies it |
| --- | --- |
| At least three writer types or access patterns | Append writer, mutation writers, and maintenance writer |
| Known-correct final-state oracle | Independently generated event rows and counter sums, compared with the final snapshot |
| Multiple runs per scenario | Ten fresh runs per scenario and policy, with reproducible randomized scheduling |
| Quantify serialization cost | A full-serialization control uses identical operation intents and initial files |
| Disjoint and overlapping regions | Separate partitions, separate keys in shared files, and shared counter keys |

## Dataset and immutable operation plan

Suggested starting scale, to calibrate before measured runs:

- Eight partitions, named p0 through p7.
- 1,000 seeded counter rows per partition; all counters start at zero.
- Columns: partition_id, row_kind, row_id, counter_value, event_payload. Counter and event rows have distinct IDs and row_kind values.
- Seed approximately ten small Parquet files per partition so maintenance has real rewrite work. Verify and record the actual file layout.
- Generate every append batch and counter-update intent before the run. Record run_id, operation_id, writer_id, partition, target keys, payload/delta, and scheduling seed in an immutable input file.
- Use a common initial snapshot/file layout for every policy within a scenario. Recreate it before each repetition.

Calibrate operation counts so workers remain active together for at least 30 seconds. Freeze these counts before comparisons. Record any resulting values instead of assuming the starting scale achieves this duration.

## Concurrent workers

Start four independent workers with a barrier. Each worker executes its own operations sequentially; the workers run concurrently.

| Worker | Role | Starting workload |
| --- | --- | --- |
| A | Streaming-style append | 40 transactions, each inserting 25 unique predetermined event rows |
| B | Backfill/mutation | 100 transactions; each increments ten seeded counters by 1 |
| C | Competing mutation | 100 transactions; each increments ten seeded counters by 1 |
| D | Maintenance | Ten scheduled compaction attempts while A–C are active |

B and C must perform transactional increments against the table snapshot, such as counter_value = counter_value + 1. Reading a value into the client and later writing an absolute replacement would change the operation semantics and can introduce application-level lost updates.

Maintenance must preserve logical rows. Count actual metadata/data commits separately from maintenance job completions; a no-op compaction is not a successful rewrite commit. If available, configure target file groups so repeated attempts have useful work, and record attempts that find no eligible files.

## Scenarios

### S1: Disjoint partitions

- A appends to p0.
- B increments keys in p1; C increments keys in p2.
- D compacts p3, which already contains small seeded files.
- Use explicit target partition predicates for mutation and maintenance scope in the scoped policy. The default policy retains its ordinary generated query without additional conflict-avoidance tuning; save both query forms.

Expected logical behavior: all operations commute. This scenario measures useful concurrency when writers can operate independently. A default query may still scan broadly, so logical partition separation does not by itself prove physical independence.

### S2: Disjoint keys in shared files

- A appends event rows to p0.
- B increments counter keys 0–9 in p0; C increments keys 10–19 in p0.
- D compacts p0.
- Seed B's and C's keys into overlapping source files and record their initial file membership.

Expected logical behavior: B and C commute, and maintenance changes no values. File-level validation may still cause conflicts. This scenario tests the gap between logical independence and physical rewrite scope.

### S3: Overlapping keys

- A appends event rows to p0.
- B and C both increment counter keys 0–9 in p0.
- D compacts p3 to isolate the primary mutation contention.

Expected logical behavior: both increments must survive when both logical operations commit. For each hot key, the expected value is the number of accepted increments affecting that key. A stale committed replacement that drops an increment must fail verification.

Example: if both mutation workers finish all 100 transactions successfully, every hot key must equal 200. If 70 transactions from B and 80 from C are durably accepted, each hot key must equal 150; the remaining 50 operations must also be reported as unfinished/aborted.

### S4: Overlapping keys with active maintenance

Use S3, but D compacts p0. Compare directly with S3 to quantify maintenance interference. Record how many maintenance attempts actually overlapped active mutations and how many rewrote files.

Expected logical behavior: the same counter sums and event rows as the accepted data operations require. Maintenance may fail validation or force retries; it must never erase accepted updates.

Add local manifest rewrite as a separate maintenance subscenario. Do not mix it with compaction in one result because it changes metadata rather than data files.

## Policies to compare

| Policy | Configuration |
| --- | --- |
| P0: Naive baseline | Engine default commit behavior; no explicit application retries, ownership, queue, or maintenance scheduling strategy |
| P1: Narrow scopes | Explicit partition predicates; otherwise the same workload and retry behavior |
| P2: Safe retries | P1 plus bounded exponential backoff with jitter, fresh transaction planning, and an atomic idempotency mechanism |
| P3: Selective coordination | P2 plus queues/ownership only for conflicting partitions or keys; coordinate maintenance with the affected region |
| P4: Full serialization control | One transaction at a time across all four workers, using the same bounded retry/idempotency settings as P2 |
| P5: Row-level MVCC | Append delta Parquet files and validate only target row versions; compaction validates the partition |

P0 performs no application retry. The simulator exposes every validation conflict directly, so record retries and full logical re-executions separately.

For P2–P4, pin an application retry budget, for example five retries with a 100 ms initial delay, doubling up to 2 seconds with jitter. These are proposed settings, not engine defaults. Resolve unknown commit outcomes before retrying.

Retrying counter increments requires more than an external audit log. The local scripts update a per-writer sequence marker atomically with each counter and process each writer's sequences in order.

P4 measures the throughput cost of serialization. Compare P3 against both P2 and P4; do not infer that queueing wins from correctness alone. Compare P5 with P2 in S2 and S3 to distinguish disjoint-row concurrency from true write-write conflicts.

## Independent oracle and pass/fail rules

Maintain three evidence sets: immutable planned operations, worker attempt receipts, and independently verified durable commit identities. Use the simulator's atomic commit history and operation IDs to resolve receipts; classify unresolved outcomes explicitly.

The oracle computes the expected final state from initial rows and distinct accepted logical operations:

1. Every seeded counter row remains present.
2. For each counter key, expected value = initial value + sum of deltas from accepted operations targeting that key.
3. Expected event rows are the exact generated rows belonging to accepted append operations, each appearing once.
4. Maintenance contributes zero logical changes.
5. Compare the full final row multiset, including duplicate, missing, unexpected, and mismatched rows.

Stop writers, resolve outcomes, capture the final table version/snapshot, and read that fixed snapshot for comparison. Preserve required history and data files until verification finishes.

Define LostUpdateCount as the sum, over counter keys, of max(expected_value - actual_value, 0); treat a missing seeded counter as actual zero and also flag it as a wrong row. Because increments are all +1, this counts missing increment effects. Define WrongFinalRows as the full-row multiset symmetric-difference count, so both missing and unexpected rows count, including duplicates. Report both counts; they may overlap.

A run passes correctness only when LostUpdateCount = 0, WrongFinalRows = 0, and all commit outcomes are resolved. Unresolved outcomes make the run inconclusive and ineligible for a throughput claim. A correct final state with permanently aborted planned operations can be reported, but include its completion ratio and abort count. Never treat abandoned work as completed.

## Repetition and timing protocol

For each scenario/policy pair:

1. Restore initial rows and file layout; record the initial version/snapshot.
2. Load the same immutable intents for the paired policy comparison.
3. Prepare workers and wait at the start barrier.
4. Release workers together and use a central monotonic clock for run duration.
5. Record each attempt, retry delay, commit resolution, abort, and maintenance no-op.
6. Join all workers, resolve uncertain outcomes, and freeze the final snapshot.
7. Run the independent oracle and save its diff.
8. Repeat ten times with different seeds, reusing each seed across policies. Randomize policy execution order to reduce environmental bias.

Use t0 = barrier release and t1 = resolution of the final scheduled operation, including failures and retries. Throughput duration is t1 - t0; exclude initial setup and final oracle execution. Record outcome-reconciliation time as part of resolution. Use a fixed overall run deadline and report timed-out work explicitly.

## Required result fields

| Field | Definition |
| --- | --- |
| CorrectCommitThroughput | Unique successful durable commits / duration; valid only after correctness passes |
| DataCommitThroughput | Successful data commits / duration |
| MaintenanceCommitThroughput | Successful maintenance commits / duration |
| LogicalOperationThroughput | Distinct completed logical operations / duration; duplicate retry acknowledgements do not count |
| CompletionRatio | Completed planned operations / total planned operations, also reported by worker |
| ConflictRate | Attempts ending in a classified concurrency conflict / total commit attempts |
| RetryCount | Internal metadata retries and application operation retries reported separately |
| P95CommitLatency | p95 from first attempt to final resolution, including queueing/backoff; also report attempt latency |
| AbortedTransactions | Permanently aborted logical operations, separated from failed attempts |
| WriteAmplification | Physical bytes written, including retries and maintenance / committed logical payload bytes; report raw bytes for zero denominators |
| Oracle results | LostUpdateCount, WrongFinalRows, unresolved outcomes, and saved row diff |
| Maintenance overlap | Attempts/commits while mutations were active, plus rewritten files/bytes and no-op attempts |

Show each run's correctness and throughput, then median throughput and spread across passing repetitions. Report failed/inconclusive repetition counts prominently. Provide p95 latency by writer role; document the percentile calculation and sample count.

## Optional privacy-delete extension

Add a fifth worker deleting predetermined seeded keys that other writers do not recreate. This yields a deterministic oracle: deleted keys must be absent after a successful delete, while remaining keys follow the increment rules. Mutations targeting deleted rows must record actual affected rows and their defined semantics.

If testing delete-versus-reinsertion, first define an ordering policy. Replay accepted operations in verified commit order or enforce a persistent deletion rule. Neither table format's ACID property alone means a later append can never recreate a deleted key.

## Local storage and maintenance validity

Run on a local filesystem that supports atomic replacement and POSIX advisory locks. Record the OS, filesystem, Python version, and Polars version. Do not generalize the results to Delta Lake or Iceberg.

Keep deletion of retained snapshots and old files outside the primary writer scenarios. Perform cleanup only after oracle verification; cleanup does not substitute for compaction or mutation conflict testing.

## Evidence to save

Save the pinned configuration, operation intents, seed, initial file layout, query forms, attempt logs, durable commit attribution, final snapshot identity, oracle diff, and per-run metrics. A result is reproducible only when another run can use those inputs and the same correctness rule.
