"""Local Polars workloads with an optimistic, file-backed commit protocol."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import sys

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import platform
import queue as queue_module
import random
import time
import uuid

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from approach import POLICIES, get_approach

SCENARIOS = {
    "s1": ("disjoint_partitions", "p1", "p2", False, False),
    "s2": ("shared_files_disjoint_keys", "p0", "p0", True, False),
    "s3": ("overlapping_keys", "p0", "p0", False, False),
    "s4": ("overlapping_keys_compaction", "p0", "p0", True, False),
    "s5": ("overlapping_keys_manifest_rewrite", "p0", "p0", True, False),
    "s6": ("privacy_delete", "p0", "p0", True, True),
}
ROW_FIELDS = (
    "partition_id", "row_kind", "row_id", "counter_value",
    "event_payload", "last_b", "last_c",
)


class CommitConflict(RuntimeError):
    def __init__(self, message, bytes_written=0):
        super().__init__(message)
        self.bytes_written = bytes_written


@dataclass(frozen=True)
class Operation:
    id: str
    writer: str
    kind: str
    partition: str
    sequence: int
    keys: tuple[int, ...] = ()
    events: tuple[tuple, ...] = ()
    delay: float = 0.0


def dump(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n")
    os.replace(temporary, path)


def load(path):
    return json.loads(Path(path).read_text())


@contextmanager
def exclusive_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        with path.open("a+b") as handle:
            while True:
                try:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(0.01)
            try:
                yield
            finally:
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
    else:
        with path.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def percentile(values, p=0.95):
    return sorted(values)[max(0, math.ceil(p * len(values)) - 1)] if values else None


def initial_rows(args):
    return [
        {
            "partition_id": f"p{partition}", "row_kind": "counter",
            "row_id": f"p{partition}:counter:{key}", "counter_value": 0,
            "event_payload": None, "last_b": 0, "last_c": 0,
            "_deleted": False,
        }
        for partition in range(args.partitions)
        for key in range(args.rows_per_partition)
    ]


def make_plan(scenario, args, seed):
    _, b_partition, c_partition, active_maintenance, privacy = SCENARIOS[scenario]
    operations = []
    counts = (
        ("A", args.append_commits), ("B", args.mutation_commits),
        ("C", args.mutation_commits), ("D", args.maintenance_attempts),
        ("E", 1 if privacy else 0),
    )
    for writer, count in counts:
        rng = random.Random(f"{seed}:{writer}")
        for sequence in range(1, count + 1):
            operation_id = f"{scenario}-{seed}-{writer}-{sequence}"
            if writer == "A":
                events = tuple(
                    (
                        "p0", "event", f"{operation_id}:event:{index}", None,
                        f"payload:{seed}:{sequence}:{index}", 0, 0,
                    )
                    for index in range(args.batch_size)
                )
                operation = Operation(operation_id, writer, "append", "p0", sequence, events=events)
            elif writer in ("B", "C"):
                first_key = 10 if scenario == "s2" and writer == "C" else 0
                partition = b_partition if writer == "B" else c_partition
                operation = Operation(
                    operation_id, writer, "increment", partition, sequence,
                    tuple(range(first_key, first_key + args.keys_per_mutation)),
                )
            elif writer == "D":
                kind = "manifest" if scenario == "s5" else "compact"
                partition = "p0" if active_maintenance else "p3"
                operation = Operation(operation_id, writer, kind, partition, sequence)
            else:
                keys = tuple(range(args.rows_per_partition - args.delete_keys, args.rows_per_partition))
                operation = Operation(operation_id, writer, "delete", "p0", sequence, keys)
            operations.append(Operation(
                **{**asdict(operation), "delay": rng.uniform(0, args.jitter_ms / 1000)}
            ))
    return operations


def operation_dict(operation):
    value = asdict(operation)
    value["keys"] = list(value["keys"])
    value["events"] = [list(row) for row in value["events"]]
    return value


def row_from_event(event):
    return {**dict(zip(ROW_FIELDS, event)), "_deleted": False}


def frame_from_rows(rows):
    import polars as pl
    schema = {
        "partition_id": pl.String, "row_kind": pl.String, "row_id": pl.String,
        "counter_value": pl.Int64, "event_payload": pl.String,
        "last_b": pl.Int64, "last_c": pl.Int64, "_deleted": pl.Boolean,
    }
    return pl.DataFrame(rows, schema=schema)


class LocalTable:
    def __init__(self, root):
        self.root = Path(root)
        self.data = self.root / "data"
        self.staging = self.root / "staging"
        self.locks = self.root / "locks"
        self.metadata_path = self.root / "metadata.json"

    def setup(self, rows, seed_files):
        self.data.mkdir(parents=True)
        self.staging.mkdir()
        self.locks.mkdir()
        partitions = {}
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["partition_id"]].append(row)
        for partition, partition_rows in grouped.items():
            files = []
            for index in range(seed_files):
                chunk = partition_rows[index::seed_files]
                if not chunk:
                    continue
                path = self.data / f"{partition}-seed-{index:03d}.parquet"
                frame_from_rows(chunk).write_parquet(path)
                files.append(str(path.relative_to(self.root)))
            partitions[partition] = {
                "version": 0, "files": files,
                "row_versions": {row["row_id"]: 0 for row in partition_rows},
            }
        dump(self.metadata_path, {
            "version": 0, "manifest_generation": 0,
            "partitions": partitions, "commits": [],
        })

    def metadata(self):
        return load(self.metadata_path)

    def read_partition(self, metadata, partition):
        import polars as pl
        files = [self.root / item for item in metadata["partitions"][partition]["files"]]
        # Files are ordered oldest to newest. Later MVCC deltas replace earlier
        # row versions; tombstones hide deleted rows from the visible snapshot.
        visible = {}
        for path in files:
            for row in pl.read_parquet(path).to_dicts():
                if row["_deleted"]:
                    visible.pop(row["row_id"], None)
                else:
                    visible[row["row_id"]] = row
        return list(visible.values())

    def read_all(self, metadata=None):
        metadata = metadata or self.metadata()
        rows = []
        for partition in sorted(metadata["partitions"]):
            rows.extend(self.read_partition(metadata, partition))
        return rows

    def stage(self, operation, rows):
        path = self.staging / f"{operation.id}-{uuid.uuid4().hex}.parquet"
        frame_from_rows(rows).write_parquet(path)
        return path

    def coordination_path(self, operation, policy):
        scope = get_approach(policy).coordination_scope(operation)
        if scope == "global":
            return self.locks / "coord-global.lock"
        if scope == "partition":
            return self.locks / f"coord-{operation.partition}.lock"
        return None

    def attempt(self, operation, policy, work_ms):
        coordination = self.coordination_path(operation, policy)
        if coordination:
            with exclusive_lock(coordination):
                return self._attempt(operation, policy, work_ms)
        return self._attempt(operation, policy, work_ms)

    def _attempt(self, operation, policy, work_ms):
        approach = get_approach(policy)
        base = self.metadata()
        if operation.id in {item["operation_id"] for item in base["commits"]}:
            return {"status": "already_committed", "bytes_written": 0}

        staged = None
        affected_ids = set()
        if operation.kind == "append":
            rows = [row_from_event(event) for event in operation.events]
            affected_ids = {row["row_id"] for row in rows}
            staged = self.stage(operation, rows)
        elif operation.kind in ("increment", "delete", "compact"):
            rows = self.read_partition(base, operation.partition)
            if operation.kind == "increment":
                key_ids = {f"{operation.partition}:counter:{key}" for key in operation.keys}
                affected_ids = key_ids
                marker = "last_b" if operation.writer == "B" else "last_c"
                safe = approach.idempotent_writes
                updated_rows = []
                for row in rows:
                    if row["row_id"] in key_ids and (not safe or row[marker] < operation.sequence):
                        row["counter_value"] += 1
                        row[marker] = operation.sequence
                        updated_rows.append(row)
                if approach.delta_mutations:
                    rows = updated_rows
            elif operation.kind == "delete":
                key_ids = {f"{operation.partition}:counter:{key}" for key in operation.keys}
                affected_ids = key_ids
                if approach.delta_mutations:
                    rows = [
                        {**row, "_deleted": True}
                        for row in rows if row["row_id"] in key_ids
                    ]
                else:
                    rows = [row for row in rows if row["row_id"] not in key_ids]
            else:
                rows.sort(key=lambda row: (row["row_kind"], row["row_id"]))
            staged = self.stage(operation, rows)

        if work_ms:
            time.sleep(work_ms / 1000)

        try:
            with exclusive_lock(self.locks / "metadata.lock"):
                current = self.metadata()
                committed = {item["operation_id"] for item in current["commits"]}
                if operation.id in committed:
                    if staged:
                        staged.unlink(missing_ok=True)
                    return {"status": "already_committed", "bytes_written": 0}

                validation = approach.validate(base, current, operation, affected_ids)
                if not validation.valid:
                    raise CommitConflict(
                        f"snapshot changed while committing {operation.id}",
                        staged.stat().st_size if staged else 0,
                    )

                next_version = current["version"] + 1
                bytes_written = 0
                if staged:
                    final = self.data / f"{operation.partition}-v{next_version:08d}-{operation.id}.parquet"
                    os.replace(staged, final)
                    bytes_written = final.stat().st_size
                    relative = str(final.relative_to(self.root))
                    partition_state = current["partitions"][operation.partition]
                    if approach.install_mode(operation) == "append":
                        partition_state["files"].append(relative)
                    else:
                        partition_state["files"] = [relative]
                    partition_state["version"] += 1
                    if operation.kind in ("append", "increment", "delete"):
                        for row_id in affected_ids:
                            partition_state["row_versions"][row_id] = next_version
                else:
                    current["manifest_generation"] += 1

                current["version"] = next_version
                current["commits"].append({
                    "version": next_version, "operation_id": operation.id,
                    "writer": operation.writer, "kind": operation.kind,
                    "partition": operation.partition,
                    "validation_scope": validation.scope,
                    "validated_row_count": validation.validated_row_count,
                    "bytes_written": bytes_written, "committed_at_ns": time.time_ns(),
                })
                dump(self.metadata_path, current)
                return {"status": "committed", "bytes_written": bytes_written}
        except CommitConflict:
            if staged:
                staged.unlink(missing_ok=True)
            raise

    def physical_inventory(self):
        return {str(path.relative_to(self.root)): path.stat().st_size for path in self.data.glob("*.parquet")}


def oracle(seed_rows, operations, committed_ids, actual_rows):
    expected = {row["row_id"]: dict(row) for row in seed_rows}
    for operation in operations:
        if operation.id not in committed_ids:
            continue
        if operation.kind == "append":
            for event in operation.events:
                row = row_from_event(event)
                expected[row["row_id"]] = row
        elif operation.kind == "increment":
            marker = "last_b" if operation.writer == "B" else "last_c"
            for key in operation.keys:
                row_id = f"{operation.partition}:counter:{key}"
                if row_id in expected:
                    expected[row_id]["counter_value"] += 1
                    expected[row_id][marker] = max(expected[row_id][marker], operation.sequence)
        elif operation.kind == "delete":
            for key in operation.keys:
                expected.pop(f"{operation.partition}:counter:{key}", None)

    def canonical(row):
        return tuple(row[field] for field in ROW_FIELDS)

    wanted = Counter(canonical(row) for row in expected.values())
    actual = Counter(canonical(row) for row in actual_rows)
    missing = wanted - actual
    unexpected = actual - wanted
    actual_by_id = defaultdict(list)
    for row in actual_rows:
        actual_by_id[row["row_id"]].append(row)
    lost_updates = 0
    for row_id, row in expected.items():
        if row["row_kind"] == "counter":
            actual_value = sum(item["counter_value"] for item in actual_by_id[row_id])
            lost_updates += max(row["counter_value"] - actual_value, 0)
    return {
        "LostUpdateCount": lost_updates,
        "WrongFinalRows": sum(missing.values()) + sum(unexpected.values()),
        "missing": [{"row": list(row), "count": count} for row, count in missing.items()],
        "unexpected": [{"row": list(row), "count": count} for row, count in unexpected.items()],
    }


def emit(event_queue, origin, writer, status, operation=None, **details):
    if event_queue is None:
        return
    event_queue.put({
        "at": time.monotonic() - origin,
        "pid": os.getpid(), "worker": writer, "status": status,
        "transaction": operation.id if operation else None,
        "kind": operation.kind if operation else None,
        "partition": operation.partition if operation else None,
        **details,
    })


def render_event(event):
    transaction = event.get("transaction") or "-"
    operation = event.get("kind") or "-"
    partition = event.get("partition") or "-"
    attempt = event.get("attempt")
    detail = f" attempt={attempt}" if attempt is not None else ""
    if event.get("wait_ms") is not None:
        detail += f" wait={event['wait_ms']:.1f}ms"
    if event.get("error"):
        detail += f" error={event['error']}"
    print(
        f"[{event['at']:8.3f}s] pid={event['pid']:<7} "
        f"worker={str(event['worker']):<2} status={event['status']:<10} "
        f"tx={transaction} kind={operation:<9} partition={partition}{detail}",
        flush=True,
    )


def render_snapshot(latest, elapsed, counts):
    """Print one bounded line for a window while preserving raw events elsewhere."""
    totals = " ".join(
        f"{status.lower()}={counts[status]}"
        for status in ("BEGIN", "CONFLICT", "RETRY", "COMMIT", "ABORT", "ERROR")
        if counts[status]
    )
    workers = []
    for worker, event in sorted(latest.items()):
        transaction = event.get("transaction") or "-"
        attempt = f" a={event['attempt']}" if event.get("attempt") is not None else ""
        workers.append(
            f"{worker}(pid={event['pid']})={event['status']}:{transaction}{attempt}"
        )
    summary = totals or f"events={sum(counts.values())}"
    print(f"[{elapsed:8.3f}s] {summary} | " + " | ".join(workers), flush=True)


def execute_operation(table, operation, policy, args, event_queue, origin):
    started = time.monotonic()
    attempts = 0
    conflicts = 0
    physical_attempt_bytes = 0
    error = None
    result = None
    approach = get_approach(policy)
    maximum_attempts = args.retry_limit + 1 if approach.retry_enabled else 1
    for attempt in range(maximum_attempts):
        attempts += 1
        emit(event_queue, origin, operation.writer, "BEGIN", operation, attempt=attempts)
        try:
            result = table.attempt(operation, policy, args.transaction_work_ms)
            physical_attempt_bytes += result["bytes_written"]
            terminal = "COMMIT" if result["status"] == "committed" else "DUPLICATE"
            emit(event_queue, origin, operation.writer, terminal, operation, attempt=attempts)
            break
        except CommitConflict as exception:
            conflicts += 1
            physical_attempt_bytes += exception.bytes_written
            error = str(exception)
            emit(
                event_queue, origin, operation.writer, "CONFLICT", operation,
                attempt=attempts, error=str(exception),
            )
            if attempt + 1 >= maximum_attempts:
                result = {"status": "aborted_conflict", "bytes_written": 0}
                emit(event_queue, origin, operation.writer, "ABORT", operation, attempt=attempts)
                break
            ceiling = min(args.retry_max_ms, args.retry_initial_ms * (2 ** attempt))
            wait_ms = random.uniform(0, ceiling)
            emit(
                event_queue, origin, operation.writer, "RETRY", operation,
                attempt=attempts + 1, wait_ms=wait_ms,
            )
            time.sleep(wait_ms / 1000)
        except Exception as exception:
            error = f"{type(exception).__name__}: {exception}"
            result = {"status": "aborted_error", "bytes_written": 0}
            emit(event_queue, origin, operation.writer, "ERROR", operation, attempt=attempts, error=error)
            break
    return {
        "operation_id": operation.id, "writer": operation.writer,
        "kind": operation.kind, "partition": operation.partition,
        "status": result["status"], "attempts": attempts, "conflicts": conflicts,
        "latency_seconds": time.monotonic() - started,
        "bytes_written": result["bytes_written"], "error": error,
        "physical_attempt_bytes": physical_attempt_bytes,
    }


def worker_main(table_root, operations, policy, args, barrier, receipt_path, event_queue, origin):
    table = LocalTable(table_root)
    receipts = []
    writer = operations[0].writer if operations else "-"
    try:
        emit(event_queue, origin, writer, "READY")
        barrier.wait(timeout=args.deadline_seconds)
        emit(event_queue, origin, writer, "RUNNING")
        for operation in operations:
            if operation.delay:
                time.sleep(operation.delay)
            receipts.append(execute_operation(table, operation, policy, args, event_queue, origin))
        emit(event_queue, origin, writer, "DONE")
    except Exception as exception:
        emit(event_queue, origin, writer, "ERROR", error=f"{type(exception).__name__}: {exception}")
        receipts.append({
            "operation_id": None, "writer": operations[0].writer if operations else None,
            "kind": "worker", "partition": None, "status": "worker_error",
            "attempts": 0, "conflicts": 0, "latency_seconds": 0,
            "bytes_written": 0, "physical_attempt_bytes": 0,
            "error": f"{type(exception).__name__}: {exception}",
        })
    dump(receipt_path, receipts)


def parse_args(scenario, argv=None):
    parser = argparse.ArgumentParser(description=f"Run local Polars scenario {scenario.upper()}")
    parser.add_argument("--output", type=Path, default=Path("results"))
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--policies", default=",".join(POLICIES))
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--rows-per-partition", type=int, default=1000)
    parser.add_argument("--seed-files", type=int, default=10)
    parser.add_argument("--append-commits", type=int, default=40)
    parser.add_argument("--mutation-commits", type=int, default=100)
    parser.add_argument("--maintenance-attempts", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--keys-per-mutation", type=int, default=10)
    parser.add_argument("--delete-keys", type=int, default=10)
    parser.add_argument("--jitter-ms", type=int, default=20)
    parser.add_argument("--transaction-work-ms", type=int, default=5,
                        help="Delay between snapshot preparation and commit to expose races")
    parser.add_argument("--retry-limit", type=int, default=5)
    parser.add_argument("--retry-initial-ms", type=int, default=10)
    parser.add_argument("--retry-max-ms", type=int, default=250)
    parser.add_argument("--deadline-seconds", type=int, default=600)
    parser.add_argument("--quiet", action="store_true",
                        help="Disable the live worker/transaction event stream")
    parser.add_argument("--event-mode", choices=("snapshot", "stream"), default="snapshot",
                        help="snapshot coalesces worker states; stream prints every event")
    parser.add_argument("--event-rate", type=float, default=4.0, metavar="HZ",
                        help="Snapshot refresh rate; a final state is also flushed (default: 4)")
    args = parser.parse_args(argv)
    args.policies = [item.strip().lower() for item in args.policies.split(",") if item.strip()]
    if args.runs < 1 or not args.policies or any(item not in POLICIES for item in args.policies):
        parser.error(
            "runs must be positive and policies must be selected from "
            + ",".join(POLICIES)
        )
    if args.partitions < 4 or args.rows_per_partition < 20 or args.seed_files < 1:
        parser.error("partitions must be >=4, rows-per-partition >=20, and seed-files positive")
    if args.keys_per_mutation > args.rows_per_partition or args.delete_keys > args.rows_per_partition:
        parser.error("key counts cannot exceed rows-per-partition")
    if args.event_rate <= 0:
        parser.error("event-rate must be greater than zero")
    return args


def one_run(scenario, args, policy, repetition, seed):
    run_id = f"{scenario}-{policy}-r{repetition}-{uuid.uuid4().hex[:8]}"
    output = args.output / run_id
    output.mkdir(parents=True, exist_ok=False)
    table = LocalTable(output / "table")
    seed_rows = initial_rows(args)
    table.setup(seed_rows, args.seed_files)
    operations = make_plan(scenario, args, seed)
    dump(output / "config.json", {
        "run_id": run_id, "scenario": scenario, "policy": policy,
        "repetition": repetition, "seed": seed, "arguments": vars(args),
        "implementation": "local-polars-optimistic-commit-simulator",
    })
    dump(output / "operations.json", [operation_dict(operation) for operation in operations])
    initial_inventory = table.physical_inventory()
    dump(output / "initial-files.json", initial_inventory)

    by_writer = defaultdict(list)
    for operation in operations:
        by_writer[operation.writer].append(operation)
    # Polars owns a thread pool; spawn avoids inheriting it into child processes.
    context = mp.get_context("spawn")
    barrier = context.Barrier(len(by_writer) + 1)
    event_queue = None if args.quiet else context.Queue()
    event_origin = time.monotonic()
    receipt_paths = {
        writer: output / f"attempts-{writer}.json" for writer in by_writer
    }
    processes = [
        context.Process(
            target=worker_main,
            args=(
                table.root, writer_operations, policy, args, barrier,
                receipt_paths[writer], event_queue, event_origin,
            ),
            name=f"writer-{writer}",
        )
        for writer, writer_operations in sorted(by_writer.items())
    ]
    if not args.quiet:
        print(
            f"\n=== scenario={scenario} policy={policy} repetition={repetition} "
            f"view={args.event_mode}@{args.event_rate:g}Hz run={run_id} ===",
            flush=True,
        )
    for process in processes:
        process.start()
    barrier.wait(timeout=args.deadline_seconds)
    started = time.monotonic()
    deadline = started + args.deadline_seconds
    events = []
    latest = {}
    window_counts = Counter()
    render_interval = 1 / args.event_rate
    next_render = time.monotonic() + render_interval

    def accept_event(event):
        events.append(event)
        if args.event_mode == "stream":
            render_event(event)
            return
        latest[event["worker"]] = event
        window_counts[event["status"]] += 1

    def render_due(force=False):
        nonlocal next_render
        if args.quiet or args.event_mode != "snapshot" or not window_counts:
            return
        now = time.monotonic()
        if force or now >= next_render:
            render_snapshot(latest, now - event_origin, window_counts)
            window_counts.clear()
            next_render = now + render_interval

    while any(process.is_alive() for process in processes) and time.monotonic() < deadline:
        if event_queue is None:
            time.sleep(0.05)
            continue
        try:
            event = event_queue.get(timeout=0.1)
            accept_event(event)
        except queue_module.Empty:
            pass
        render_due()
    for process in processes:
        process.join(timeout=0.1)
    timed_out = [process.name for process in processes if process.is_alive()]
    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join()
    if event_queue is not None:
        while True:
            try:
                event = event_queue.get_nowait()
                accept_event(event)
            except queue_module.Empty:
                break
    render_due(force=True)
    failed_workers = [
        {"name": process.name, "exitcode": process.exitcode}
        for process in processes if process.exitcode not in (0, None)
    ]
    duration = max(time.monotonic() - started, 0.000001)

    receipts = []
    for receipt_path in receipt_paths.values():
        if receipt_path.exists():
            receipts.extend(load(receipt_path))
    metadata = table.metadata()
    committed_ids = {entry["operation_id"] for entry in metadata["commits"]}
    actual_rows = table.read_all(metadata)
    oracle_result = oracle(seed_rows, operations, committed_ids, actual_rows)
    worker_errors = [item for item in receipts if item["status"] == "worker_error"]
    correctness = (
        not timed_out and not failed_workers and not worker_errors
        and oracle_result["LostUpdateCount"] == 0
        and oracle_result["WrongFinalRows"] == 0
    )
    data_operations = [operation for operation in operations if operation.kind not in ("compact", "manifest")]
    committed_data_ids = {operation.id for operation in data_operations if operation.id in committed_ids}
    physical_bytes = sum(item["physical_attempt_bytes"] for item in receipts)
    logical_bytes = sum(
        len(json.dumps(operation_dict(operation), separators=(",", ":")).encode())
        for operation in data_operations if operation.id in committed_ids
    )
    final_inventory = table.physical_inventory()
    active_files = {item for partition in metadata["partitions"].values() for item in partition["files"]}
    metrics = {
        "scenario": scenario, "policy": policy, "repetition": repetition,
        "seed": seed, "duration_seconds": duration,
        "correctness_pass": correctness,
        "CorrectCommitThroughput": len(metadata["commits"]) / duration if correctness else None,
        "successful_commits": len(metadata["commits"]),
        "data_commits": sum(entry["kind"] not in ("compact", "manifest") for entry in metadata["commits"]),
        "maintenance_commits": sum(entry["kind"] in ("compact", "manifest") for entry in metadata["commits"]),
        "planned_data_operations": len(data_operations),
        "committed_data_operations": len(committed_data_ids),
        "completion_ratio": len(committed_data_ids) / max(1, len(data_operations)),
        "conflict_count": sum(item["conflicts"] for item in receipts),
        "retry_count": sum(max(0, item["attempts"] - 1) for item in receipts),
        "p95_commit_latency_seconds": percentile([item["latency_seconds"] for item in receipts if item["operation_id"]]),
        "aborted_operations": [item for item in receipts if item["status"].startswith("aborted")],
        "timed_out_workers": timed_out, "failed_workers": failed_workers,
        "worker_errors": worker_errors,
        "LostUpdateCount": oracle_result["LostUpdateCount"],
        "WrongFinalRows": oracle_result["WrongFinalRows"],
        "oracle": oracle_result,
        "physical_bytes_written": physical_bytes,
        "committed_logical_intent_bytes": logical_bytes,
        "write_amplification": physical_bytes / max(1, logical_bytes),
        "physical_file_count": len(final_inventory),
        "active_file_count": len(active_files),
        "retained_old_or_orphan_file_count": len(final_inventory) - len(active_files),
        "final_metadata_version": metadata["version"],
        "mvcc_delta_commits": sum(
            entry["validation_scope"] == "row" for entry in metadata["commits"]
        ),
        "tracked_row_versions": sum(
            len(partition["row_versions"]) for partition in metadata["partitions"].values()
        ),
        "commit_history": metadata["commits"],
        "environment": {
            "python": sys.version, "platform": platform.platform(),
            "polars": __import__("polars").__version__,
        },
    }
    dump(output / "attempts.json", receipts)
    dump(output / "events.json", events)
    dump(output / "metrics.json", metrics)
    return metrics


def run(scenario, argv=None):
    args = parse_args(scenario, argv)
    args.output.mkdir(parents=True, exist_ok=True)
    master_rng = random.Random(20261004)
    results = []
    for repetition in range(1, args.runs + 1):
        seed = master_rng.randrange(1, 2**31)
        policies = list(args.policies)
        master_rng.shuffle(policies)
        for policy in policies:
            result = one_run(scenario, args, policy, repetition, seed)
            results.append(result)
            print(json.dumps({
                key: result[key] for key in (
                    "scenario", "policy", "repetition", "correctness_pass",
                    "CorrectCommitThroughput", "completion_ratio",
                    "conflict_count", "retry_count", "LostUpdateCount", "WrongFinalRows",
                )
            }), flush=True)
    dump(args.output / f"{scenario}-summary.json", results)
    return 0 if all(result["correctness_pass"] for result in results) else 2


def script_main(scenario):
    raise SystemExit(run(scenario))
