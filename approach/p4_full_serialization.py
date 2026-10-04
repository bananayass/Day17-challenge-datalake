"""P4: serialize every operation with one global coordination lock."""
from .p2_retry_occ import P2RetryOCC


class P4FullSerialization(P2RetryOCC):
    name = "p4"

    def coordination_scope(self, operation):
        return "global"

