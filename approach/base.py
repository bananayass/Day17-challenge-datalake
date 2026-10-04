"""Shared contract for local concurrency-control approaches."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Validation:
    valid: bool
    scope: str
    validated_row_count: int = 0


class Approach:
    """Strategy interface used by the workload runner."""

    name = "base"
    retry_enabled = False
    idempotent_writes = False
    delta_mutations = False

    def coordination_scope(self, operation: Any) -> str | None:
        # Manifest rewrites contend on global metadata in every approach.
        return "global" if operation.kind == "manifest" else None

    def validate(
        self,
        base: dict,
        current: dict,
        operation: Any,
        affected_ids: set[str],
    ) -> Validation:
        raise NotImplementedError

    def install_mode(self, operation: Any) -> str:
        """Return append for additive files or replace for a partition rewrite."""
        return "append" if operation.kind == "append" else "replace"

    @staticmethod
    def global_unchanged(base: dict, current: dict) -> bool:
        return current["version"] == base["version"]

    @staticmethod
    def partition_unchanged(base: dict, current: dict, partition: str) -> bool:
        return (
            current["partitions"][partition]["version"]
            == base["partitions"][partition]["version"]
        )

    @staticmethod
    def rows_unchanged(
        base: dict,
        current: dict,
        partition: str,
        affected_ids: set[str],
    ) -> bool:
        base_versions = base["partitions"][partition]["row_versions"]
        current_versions = current["partitions"][partition]["row_versions"]
        return all(
            current_versions.get(row_id) == base_versions.get(row_id)
            for row_id in affected_ids
        )

