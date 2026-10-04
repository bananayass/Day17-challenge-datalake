# Local concurrent-writer experiments

These experiments run entirely on one machine. Polars reads and writes local Parquet files, while a small file-backed transaction layer models snapshots, optimistic validation, atomic metadata replacement, retries, partition queues, and global serialization.

The transaction layer is an experiment simulator. It is not an implementation of Delta Lake or Apache Iceberg and its results must not be presented as a benchmark of either table format.

## Install

Python 3.12 and Polars are the only runtime requirements. Java, Spark, Delta Lake, Iceberg, object storage, and external catalogs are not used.

```sh
uv sync
```

You can also use a regular virtual environment:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

## Scenarios

| Script | Workload |
| --- | --- |
| `scenarios/s1_disjoint_partitions.py` | Appends, mutations, and compaction in separate partitions |
| `scenarios/s2_shared_files_disjoint_keys.py` | Different keys sharing a partition and initial files |
| `scenarios/s3_overlapping_keys.py` | Two mutation writers increment the same keys |
| `scenarios/s4_overlapping_keys_compaction.py` | Overlapping increments race with compaction |
| `scenarios/s5_manifest_rewrite.py` | Local metadata/manifest rewrite races with overlapping mutations |
| `scenarios/s6_privacy_delete.py` | Privacy deletion runs with appends, mutations, and compaction |

Run a small smoke experiment:

```sh
uv run python scenarios/s1_disjoint_partitions.py \
  --runs 1 --policies p0,p1,p2,p3,p4,p5 \
  --append-commits 5 --mutation-commits 10 --maintenance-attempts 2
```

The default CLI applies output backpressure by coalescing rapid events into the latest state for each worker. It refreshes four times per second and flushes one final state:

```text
=== scenario=s3 policy=p2 repetition=1 view=snapshot@4Hz run=s3-p2-r1-... ===
[   0.501s] begin=5 conflict=2 retry=2 commit=3 | A(pid=42107)=COMMIT:s3-...-A-1 a=1 | B(pid=42108)=RETRY:s3-...-B-1 a=3 | C(pid=42109)=COMMIT:s3-...-C-2 a=1 | D(pid=42110)=DONE:-
```

The snapshot keeps only one display state per worker, so stdout cannot grow at transaction speed. Window counters preserve visibility of short-lived conflicts and retries. This presentation layer does not block workers or alter throughput timing, and every raw event is still saved to `events.json`.

Use `--event-rate 2` for two snapshots per second, `--event-mode stream` to print every raw event, or `--quiet` for metrics-only batch execution. Raw statuses include `READY`, `RUNNING`, `BEGIN`, `CONFLICT`, `RETRY`, `COMMIT`, `DUPLICATE`, `ABORT`, `ERROR`, and `DONE`.

Run the planned experiment with ten repetitions:

```sh
uv run python scenarios/s4_overlapping_keys_compaction.py \
  --runs 10 --policies p0,p1,p2,p3,p4,p5
```

All output stays under `results/`. Each run receives a unique directory containing its immutable operation plan, initial file inventory, live events, attempt receipts, local table files, commit history, oracle diff, and metrics.

## Policies

Policy algorithms live in separate modules under [`approach/`](approach/README.md), while `scenarios/common.py` contains only workload orchestration, local storage, the oracle, and metrics.

| Policy | Local behavior |
| --- | --- |
| `p0` | Naive optimistic writer validates the global metadata version and does not retry |
| `p1` | Mutations validate only their target partition; blind appends rebase onto the current file list |
| `p2` | `p1` plus bounded retry/backoff and same-row writer sequence markers |
| `p3` | `p2` plus a queue for operations targeting the same partition |
| `p4` | One global queue serializes all operations as the throughput control |
| `p5` | Row-level MVCC writes delta files and validates only changed row versions |

Every commit briefly takes the metadata file lock so `metadata.json` can be replaced atomically. Writers do their Parquet work before this lock. P5 keeps row versions in metadata and appends mutation/delete deltas, allowing disjoint keys in one partition to commit concurrently. Compaction still validates the whole partition.

## Correctness and metrics

The oracle starts from independently generated seed rows and applies only operation IDs found in the durable commit history. It compares the complete expected and actual row multisets. A run fails correctness when `LostUpdateCount` or `WrongFinalRows` is nonzero, or a worker fails or times out.

`CorrectCommitThroughput` is reported only for a correct run. The output also includes completion ratio, conflict count, retry count, p95 commit latency, aborted operations, physical bytes written, write amplification, and retained old files.

Old Parquet files are deliberately retained so the experiment can inspect write amplification and snapshot leftovers. Remove `results/` manually after you finish reviewing the evidence.

See [baseline.md](baseline.md), [experiment-scenarios.md](experiment-scenarios.md), and [research.md](research.md) for the assignment, scenario rationale, and source research.
