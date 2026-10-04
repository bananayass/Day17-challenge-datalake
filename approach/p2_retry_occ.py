"""P2: partition OCC plus bounded retry and idempotent row markers."""
from .p1_partition_occ import P1PartitionOCC


class P2RetryOCC(P1PartitionOCC):
    name = "p2"
    retry_enabled = True
    idempotent_writes = True

