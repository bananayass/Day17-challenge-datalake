"""Generate the shared, deterministic initial dataset using Python's stdlib."""

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data"


def main():
    OUTPUT.mkdir(exist_ok=True)
    rows = [
        {
            "partition_id": f"p{partition}",
            "row_kind": "counter",
            "row_id": f"counter-p{partition}-{key:04d}",
            "counter_value": 0,
            "event_payload": None,
        }
        for partition in range(8)
        for key in range(1000)
    ]
    content = (json.dumps(rows, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    (OUTPUT / "initial-data.json").write_bytes(content)
    manifest = {
        "dataset_id": "lakehouse-concurrency-initial-v1",
        "data_file": "initial-data.json",
        "format": "JSON array",
        "encoding": "UTF-8",
        "sha256": hashlib.sha256(content).hexdigest(),
        "row_count": len(rows),
        "partition_column": "partition_id",
        "partitions": {f"p{i}": 1000 for i in range(8)},
        "schema": [
            {"name": "partition_id", "type": "string", "nullable": False},
            {"name": "row_kind", "type": "string", "nullable": False},
            {"name": "row_id", "type": "string", "nullable": False},
            {"name": "counter_value", "type": "long", "nullable": True},
            {"name": "event_payload", "type": "string", "nullable": True},
        ],
        "initial_invariants": {
            "unique_row_ids": True,
            "row_kind": "counter",
            "counter_value": 0,
            "event_payload": None,
            "event_row_count": 0,
        },
        "suggested_file_layout": {
            "files_per_partition": 10,
            "rows_per_file": 100,
            "group_rule": "key // 100",
            "shared_hot_keys": "Keys 0-19 should share a physical data file for S2.",
            "note": "JSON order does not guarantee table file layout. Verify actual files after import.",
        },
    }
    (OUTPUT / "initial-data.manifest.json").write_bytes(
        (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
    )
    print(f"Generated {len(rows)} rows; SHA-256: {manifest['sha256']}")


if __name__ == "__main__":
    main()
