"""Registry for concurrency-control approaches."""
from .p0_global_occ import P0GlobalOCC
from .p1_partition_occ import P1PartitionOCC
from .p2_retry_occ import P2RetryOCC
from .p3_partition_queue import P3PartitionQueue
from .p4_full_serialization import P4FullSerialization
from .p5_row_mvcc import P5RowMVCC

_APPROACHES = {
    approach.name: approach
    for approach in (
        P0GlobalOCC(),
        P1PartitionOCC(),
        P2RetryOCC(),
        P3PartitionQueue(),
        P4FullSerialization(),
        P5RowMVCC(),
    )
}

POLICIES = tuple(_APPROACHES)


def get_approach(name):
    try:
        return _APPROACHES[name]
    except KeyError as error:
        raise ValueError(f"Unknown policy: {name}") from error

