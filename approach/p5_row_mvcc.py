"""P5: row-level MVCC using append-only mutation and tombstone deltas."""
from .base import Approach, Validation


class P5RowMVCC(Approach):
    name = "p5"
    retry_enabled = True
    idempotent_writes = True
    delta_mutations = True

    def validate(self, base, current, operation, affected_ids):
        if operation.kind == "manifest":
            return Validation(self.global_unchanged(base, current), "global")
        if operation.kind == "compact":
            return Validation(
                self.partition_unchanged(base, current, operation.partition),
                "partition",
            )
        return Validation(
            self.rows_unchanged(
                base, current, operation.partition, affected_ids
            ),
            "row",
            len(affected_ids),
        )

    def install_mode(self, operation):
        if operation.kind in ("append", "increment", "delete"):
            return "append"
        return "replace"

