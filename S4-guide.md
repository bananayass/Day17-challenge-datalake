# Chạy và đọc thí nghiệm S4

Đây là simulator transaction dùng Polars/Parquet, không phải Delta Lake hay
Iceberg thực. Dữ liệu được tự sinh trong `common.py`, không nạp
`data/initial-data.json`.

## Các file trong scenarios

| File | Vai trò |
| --- | --- |
| `common.py` | Toàn bộ logic dùng chung: sinh dữ liệu/thao tác, ghi Parquet, snapshot metadata, khóa, kiểm tra conflict, retry, multiprocessing, oracle và metrics. |
| `s1_disjoint_partitions.py` | A append p0, B tăng p1, C tăng p2, D compaction p3. |
| `s2_shared_files_disjoint_keys.py` | B tăng khóa 0-9, C tăng khóa 10-19 trong p0; D compaction p0. |
| `s3_overlapping_keys.py` | B/C cùng tăng khóa 0-9 trong p0; D compaction p3. |
| `s4_overlapping_keys_compaction.py` | Như S3, nhưng D compaction p0 để cạnh tranh cùng vùng dữ liệu. |
| `s5_manifest_rewrite.py` | B/C cùng tăng khóa trong p0; D thay đổi metadata mô phỏng manifest rewrite. |
| `s6_privacy_delete.py` | Thêm E xóa 10 khóa cuối p0; B/C vẫn tăng 10 khóa đầu. Không kiểm tra delete-vs-reinsert. |

Các file s1-s6 chỉ là entrypoint gọi `script_main` với mã scenario; muốn hiểu
thuật toán cần đọc `common.py`.

## S4 thực hiện gì?

Mỗi lần chạy tạo 8 partition, mỗi partition 1.000 counter bằng 0, ban đầu
10 file Parquet/partition. Bốn process cùng bắt đầu qua barrier:

- A: 40 append, mỗi append 25 event vào p0.
- B: 100 giao dịch, mỗi giao dịch tăng 10 khóa đầu p0 thêm 1.
- C: giống B, nhắm đúng các khóa đó.
- D: 10 lần compaction p0; simulator đọc và viết lại partition ngay cả khi
  partition chỉ còn một file, không có cơ chế bỏ qua no-op thực tế.

Khóa counter dùng dạng `p0:counter:0`. Các cột `last_b`, `last_c` lưu sequence
để chống tăng trùng trong các policy có retry. Khi commit, simulator thay
metadata bằng `os.replace` dưới khóa ngắn; cập nhật/compaction thay danh sách
file của cả partition. File cũ được giữ lại.

| Policy | Hành vi |
| --- | --- |
| p0 | Kiểm tra version toàn bảng, không retry. |
| p1 | Update/compaction kiểm tra version partition; append dùng danh sách file hiện tại; không retry. |
| p2 | p1 + tối đa 5 retry, backoff/jitter và sequence marker. |
| p3 | p2 + khóa điều phối partition. Trong S4 mọi worker cùng p0 nên gần như tuần tự. |
| p4 | Khóa điều phối toàn bảng, chạy tuần tự. |

## Chạy trên máy Windows này

Code dùng `fcntl`, nên chạy bằng Kali WSL. Môi trường riêng
`.venv-s4-wsl` đã được chuẩn bị với Polars 1.34.0. Python WSL là 3.13.12,
khác bản 3.12 được mô tả trong README; cần thống nhất phiên bản giữa nhóm
trước khi đo chính thức.

Từ PowerShell, chạy workload đầy đủ một lần mỗi policy. Lưu output trên
filesystem Linux (ví dụ `/tmp`) thay vì ổ Windows mount: lượt thử trên
`/mnt/d` đã xuất hiện lỗi đọc metadata trong lúc ghi đồng thời.

```powershell
wsl -d kali-linux --cd '/mnt/d/Ai Thuc chien/All Lab/Day17-challenge-datalake' -- .venv-s4-wsl/bin/python scenarios/s4_overlapping_keys_compaction.py --runs 1 --policies p0,p1,p2,p3,p4 --output /tmp/day17-s4-local --event-rate 1
```

Để chạy 10 lần mỗi policy, dùng output riêng:

```powershell
wsl -d kali-linux --cd '/mnt/d/Ai Thuc chien/All Lab/Day17-challenge-datalake' -- .venv-s4-wsl/bin/python scenarios/s4_overlapping_keys_compaction.py --runs 10 --policies p0,p1,p2,p3,p4 --output /tmp/day17-s4-repeated --event-rate 1
```

`--quiet` không lưu live events (events.json sẽ rỗng); attempt receipts và
metrics vẫn được lưu. Bỏ tùy chọn này nếu cần bằng chứng sự kiện chi tiết.
Không chạy hai command cùng thư mục output đồng thời vì summary có cùng tên.

## Đọc output

Trong mỗi thư mục run:

- `config.json`: tham số, seed và policy.
- `operations.json`: toàn bộ thao tác dự kiến, dùng để tái lập workload.
- `initial-files.json`: đường dẫn và kích thước các file ban đầu.
- `events.json`: trạng thái READY/BEGIN/CONFLICT/RETRY/COMMIT/ABORT/DONE.
- `attempts-A.json` đến `attempts-D.json`: kết quả riêng từng worker.
- `attempts.json`: kết quả thao tác gộp, số attempt/conflict và độ trễ.
- `metrics.json`: correctness, throughput, completion ratio, retry, bytes,
  lịch sử commit và oracle diff (trong trường `oracle`).
- `table/metadata.json`: version cuối, danh sách file đang hoạt động, commits.
- `table/data/`: Parquet hiện tại và file cũ được giữ lại.

`s4-summary.json` là mảng kết quả các run, chưa phải bảng median tổng hợp.

Nếu B/C commit đủ 100 giao dịch mỗi worker, 10 counter nóng đều bằng 200.
Nếu A commit đủ, bảng có 9.000 dòng. Nếu có thao tác abort, oracle chỉ yêu
cầu kết quả tương ứng những thao tác được ghi trong commit history.

Vì thế `correctness_pass=true` cần đọc kèm `completion_ratio` và abort:
đúng dữ liệu không đồng nghĩa làm xong toàn bộ workload. Throughput p0/p1
có thể nhìn cao vì chúng bỏ thao tác thay vì retry.

Chỉ một lần chạy chưa đủ khẳng định policy thắng. Chạy lặp, hiệu chỉnh để
các worker cùng hoạt động >=30 giây, và báo cáo cả thất bại/abort. Độ trễ p95
hiện tại gộp các thao tác có receipt, bao gồm thao tác abort, không chỉ commit.
