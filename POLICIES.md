# Các policy quản lý ghi đồng thời

Các policy từ P0 đến P5 dùng cùng dữ liệu đầu vào và cùng nhóm worker. Điểm khác nhau là phạm vi kiểm tra xung đột, cách retry và mức độ tuần tự hóa giao dịch.

Mục tiêu so sánh là tăng `CorrectCommitThroughput` nhưng vẫn phải bảo đảm:

- `LostUpdateCount = 0`
- `WrongFinalRows = 0`

Nếu một lần chạy vi phạm một trong hai điều kiện trên, kết quả throughput của lần chạy đó không hợp lệ.

## Bảng tổng quan

| Policy | Cơ chế chính | Retry | Mức chạy song song | Vai trò trong thí nghiệm |
| --- | --- | --- | --- | --- |
| P0 | Kiểm tra version toàn bảng | Không | Có, nhưng dễ xung đột giả | Baseline đơn giản |
| P1 | Kiểm tra version theo partition | Không | Cao hơn P0 | Giảm phạm vi xung đột |
| P2 | P1 + retry/backoff + idempotency | Có giới hạn | Cao | Xử lý xung đột an toàn |
| P3 | P2 + queue theo partition | Có giới hạn | Song song giữa các partition | Chỉ tuần tự hóa vùng đang xung đột |
| P4 | Một queue chung cho toàn bảng | Có giới hạn | Không | Mốc đo chi phí tuần tự hóa toàn bộ |
| P5 | MVCC theo row, ghi delta file | Có giới hạn | Cao, kể cả trong cùng partition | Chỉ conflict khi cùng row bị thay đổi |

Implementation của từng policy được tách riêng trong thư mục [`approach/`](approach/README.md). Runner chỉ gọi contract chung để workload và cách đo metric giống nhau cho mọi policy.

## P0 — Naive baseline

P0 là cách xử lý đơn giản nhất. Mỗi transaction đọc một snapshot có `global version`, chuẩn bị file Parquet mới, rồi kiểm tra lại version trước khi commit.

Transaction chỉ commit khi global version chưa thay đổi. Nếu bất kỳ worker nào commit trước, global version thay đổi và transaction hiện tại bị từ chối, kể cả khi hai worker ghi vào hai partition khác nhau.

Ví dụ:

1. Worker B đọc `p1` tại global version 10.
2. Worker C commit thay đổi vào `p2`, làm global version tăng thành 11.
3. Worker B kiểm tra lại và bị conflict dù `p1` và `p2` không chồng lấn.

P0 không retry ở tầng ứng dụng. Transaction gặp conflict sẽ abort và worker chuyển sang transaction tiếp theo.

P0 giúp đo conflict rate và completion ratio của cách xử lý ngây thơ. Nó thường bỏ phí khả năng chạy song song của các partition độc lập.

## P1 — Narrow conflict scope

P1 thu hẹp phạm vi kiểm tra xung đột từ toàn bảng xuống partition mà transaction thay đổi.

Một mutation, delete hoặc compaction chỉ bị conflict khi version của partition mục tiêu đã thay đổi. Commit vào partition khác không làm transaction thất bại.

Blind append được xem là thao tác có thể gộp. Khi commit, append thêm file mới vào danh sách file hiện tại của partition thay vì thay thế toàn bộ danh sách. Vì vậy nhiều append không làm mất file của nhau.

P1 vẫn không retry. Transaction có xung đột thật trong cùng partition sẽ abort.

So sánh P1 với P0 cho biết thu hẹp phạm vi conflict giúp tăng throughput bao nhiêu, đặc biệt trong scenario S1 khi worker ghi vào các partition riêng biệt.

## P2 — Safe retry

P2 sử dụng phạm vi conflict của P1 và bổ sung retry có giới hạn.

Khi gặp conflict, transaction:

1. Chờ một khoảng backoff ngẫu nhiên.
2. Đọc lại metadata và dữ liệu mới nhất.
3. Tính lại kết quả transaction từ snapshot mới.
4. Thử commit lại.

Số lần retry được điều khiển bởi:

- `--retry-limit`: số lần thử lại sau lần đầu.
- `--retry-initial-ms`: giới hạn chờ ban đầu.
- `--retry-max-ms`: giới hạn chờ tối đa.

Backoff tăng dần và có jitter để các worker không liên tục thử commit cùng thời điểm.

Các increment còn lưu sequence riêng cho worker B và C trong cùng row. Sequence giúp một transaction được retry nhưng không cộng cùng một increment hai lần. Commit history cũng lưu `operation_id` để nhận biết transaction đã commit trước đó.

P2 phù hợp khi conflict xảy ra không thường xuyên và chi phí retry thấp hơn chi phí đưa mọi transaction vào queue.

## P3 — Selective coordination

P3 dùng toàn bộ cơ chế của P2 và thêm một coordination lock cho từng partition.

Các transaction nhắm cùng partition phải chờ nhau. Transaction ở các partition khác nhau vẫn chạy đồng thời.

Ví dụ:

- Worker A ghi `p0` và worker B cập nhật `p0`: chạy lần lượt.
- Worker B cập nhật `p1` và worker C cập nhật `p2`: vẫn chạy song song.

P3 giảm số lần làm việc vô ích như đọc snapshot, tạo file Parquet rồi mới phát hiện conflict. Đổi lại, transaction có thể phải chờ queue và p95 latency có thể tăng tại partition nóng.

So sánh P3 với P2 cho biết queue theo vùng có giảm retry và write amplification đủ nhiều để bù cho thời gian chờ hay không.

## P4 — Full serialization control

P4 đặt mọi transaction vào một coordination lock chung. Tại một thời điểm chỉ có một worker được thực hiện toàn bộ quá trình đọc, chuẩn bị dữ liệu và commit.

Cách này gần như loại bỏ conflict giữa các transaction trong thí nghiệm, nhưng cũng loại bỏ khả năng chạy song song giữa các partition độc lập.

P4 không được mặc định xem là giải pháp tốt nhất. Nó là control để định lượng chi phí tuần tự hóa toàn bộ:

- Nếu P4 đúng nhưng throughput thấp hơn nhiều, kết quả cho thấy serialization đã hy sinh hiệu năng.
- Nếu P3 đạt correctness tương đương và throughput cao hơn, selective coordination là lựa chọn tốt hơn.
- Nếu workload có mức xung đột rất cao, P4 đôi khi có thể cạnh tranh với retry liên tục; điều này phải được chứng minh bằng số liệu.

## P5 — Row-level MVCC

P5 dùng Multi-Version Concurrency Control ở mức row. Mỗi transaction đọc một snapshot metadata và dữ liệu tương ứng. Thay vì rewrite toàn bộ partition, transaction chỉ ghi những row thay đổi vào một delta Parquet file mới.

Metadata lưu version mới nhất của từng `row_id`. Khi commit, transaction chỉ kiểm tra version của các row nó đã đọc và muốn thay đổi:

- Nếu các row đó chưa đổi, transaction được commit dù partition đã có commit khác.
- Nếu một row mục tiêu đã đổi, transaction conflict, đọc snapshot mới và retry.
- Append với ID mới có thể commit cùng các mutation khác.
- Delete ghi tombstone như một version mới của row.

Ví dụ trong S2:

1. Worker B cập nhật key 0–9 trong `p0`.
2. Worker C đồng thời cập nhật key 10–19 trong `p0`.
3. Cả hai có thể commit vì tập row không giao nhau, dù chúng ở cùng partition và file ban đầu.

Trong S3, B và C cùng cập nhật key 0–9. Transaction commit sau sẽ phát hiện row version đã đổi, conflict rồi retry trên version mới. Nhờ đó increment của cả hai worker vẫn được giữ lại.

Compaction là trường hợp đặc biệt: nó thay thế toàn bộ tập file của partition nên phải kiểm tra version toàn partition. Nếu có writer commit trong lúc compaction chuẩn bị file, compaction phải retry từ snapshot mới.

P5 vẫn có một metadata lock rất ngắn để cập nhật metadata bằng thao tác atomic. Phần đọc và tạo delta Parquet chạy đồng thời bên ngoài lock. Vì vậy đây là MVCC mô phỏng cho experiment local, không phải implementation đầy đủ của database hoặc lakehouse production.

Điểm cần đánh giá khi so sánh P5:

- Conflict rate trong S2 phải thấp hơn policy kiểm tra theo partition.
- S3 vẫn phải phát hiện write-write conflict trên cùng key.
- Delta file làm giảm byte ghi cho mutation nhỏ nhưng tăng số file đang active.
- Compaction cần kiểm soát số file và có thể làm tăng p95 latency hoặc retry.

## Cách đọc kết quả

Không nên chỉ so sánh throughput. Với mỗi policy cần xem đồng thời:

- `correctness_pass`: oracle có xác nhận trạng thái cuối đúng hay không.
- `CorrectCommitThroughput`: số commit thành công mỗi giây của một run đúng.
- `completion_ratio`: tỷ lệ logical operation dự kiến đã commit.
- `conflict_count`: tổng số lần optimistic validation phát hiện xung đột.
- `retry_count`: tổng số lần ứng dụng chạy lại transaction.
- `p95_commit_latency_seconds`: độ trễ transaction ở percentile 95.
- `write_amplification`: số byte vật lý được ghi so với kích thước logical intent đã commit.

Một policy chỉ được xem là tốt hơn khi giữ `LostUpdateCount = 0` và `WrongFinalRows = 0`, đồng thời cải thiện throughput mà không tạo ra completion ratio hoặc p95 latency không chấp nhận được.

## Quan hệ với các scenario

- S1 kiểm tra liệu P1–P3 có tận dụng được các partition độc lập tốt hơn P0 và P4 hay không.
- S2 kiểm tra lợi ích chính của P5: các key khác nhau trong cùng partition vẫn có thể commit đồng thời.
- S3 kiểm tra P5 vẫn phát hiện xung đột thật khi hai worker increment cùng key.
- S4 thêm compaction vào partition đang được cập nhật.
- S5 chạy manifest rewrite đồng thời với mutation.
- S6 thêm privacy delete vào workload.

Chạy cùng một scenario với tất cả policy:

```bash
uv run python scenarios/s3_overlapping_keys.py \
  --runs 10 \
  --policies p0,p1,p2,p3,p4,p5
```
