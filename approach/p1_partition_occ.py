"""P1: partition-level OCC; blind appends rebase without retry."""
from .base import Approach, Validation


class P1PartitionOCC(Approach):
    name = "p1"

    def validate(self, base, current, operation, affected_ids):
        if operation.kind == "manifest":
            return Validation(self.global_unchanged(base, current), "global")
        if operation.kind == "append":
            return Validation(True, "append-rebase")
        return Validation(
            self.partition_unchanged(base, current, operation.partition),
            "partition",
        )

