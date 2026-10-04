"""P3: P2 with selective serialization inside each partition."""
from .p2_retry_occ import P2RetryOCC


class P3PartitionQueue(P2RetryOCC):
    name = "p3"

    def coordination_scope(self, operation):
        return "global" if operation.kind == "manifest" else "partition"

