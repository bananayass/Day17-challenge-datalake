# Policy strategy modules

Mỗi policy có một implementation riêng:

| Policy | File | Thuật toán |
| --- | --- | --- |
| P0 | `p0_global_occ.py` | Global optimistic concurrency control |
| P1 | `p1_partition_occ.py` | Partition-level optimistic validation |
| P2 | `p2_retry_occ.py` | Partition OCC với retry và idempotency |
| P3 | `p3_partition_queue.py` | Queue theo partition |
| P4 | `p4_full_serialization.py` | Tuần tự hóa toàn bộ transaction |
| P5 | `p5_row_mvcc.py` | Row-level MVCC và delta files |

`base.py` định nghĩa contract chung gồm:

- `validate`: xác định transaction còn hợp lệ trên metadata hiện tại hay không.
- `coordination_scope`: chọn không lock, lock theo partition hoặc global lock.
- `install_mode`: append delta file hoặc thay thế tập file của partition.
- `retry_enabled`: transaction có được retry sau conflict hay không.
- `idempotent_writes`: bật sequence marker để retry không áp dụng mutation hai lần.
- `delta_mutations`: mutation ghi delta rows hay rewrite snapshot của partition.

`__init__.py` là registry ánh xạ tên `p0`–`p5` sang strategy tương ứng. Runner trong `scenarios/common.py` lấy strategy từ registry và không chứa nhánh thuật toán riêng cho từng policy.
