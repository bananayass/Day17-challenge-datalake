# Báo cáo S4: cập nhật cùng khóa và compaction

Đã chạy 10 lần cho mỗi P0-P5 (60 run), trên simulator Polars/Parquet. Đây là phép đo workload cố định quy mô nhỏ, không phải benchmark Delta Lake/Iceberg.

## Cấu hình và cách kiểm chứng

- 8 partition × 1.000 counter, 10 file khởi tạo/partition.
- A: 40 append × 25 event; B/C: mỗi worker 100 increment × 10 khóa đầu p0; D: 10 rewrite compaction p0.
- Delay mô phỏng trong transaction: 5 ms; jitter giữa thao tác: 0-20 ms. Retry tối đa 5 lần, full jitter 10-250 ms.
- Các policy dùng cùng seed theo repetition, thứ tự policy được xáo trộn; mỗi run tạo bảng mới.
- Lưu raw data trên filesystem Linux WSL để tránh lỗi metadata trên ổ Windows mount.
- Throughput đo từ barrier release tới receipt thao tác cuối cùng, gồm retry/queue; không gồm setup, oracle và process teardown.
- Đối chiếu planned IDs, receipt IDs, commit IDs, rồi tính lại toàn bộ counter/event từ intent và đọc snapshot Parquet cuối bằng script kiểm chứng riêng.
- SHA-256 mã nguồn ở s4-provenance.json; tham số, môi trường và evidence_path của từng run ở s4-summary.json.

## Kết quả

| Policy | Run đúng | Median commit/s | Khoảng min-max | Median completion | Median conflict | Median retry |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| p2 | 10/10 | 32.04 | 29.67-33.85 | 96.7% | 183.5 | 172.5 |
| p0 | 10/10 | 31.18 | 30.68-33.38 | 53.1% | 122 | 0 |
| p1 | 10/10 | 30.97 | 29.67-32.64 | 54.6% | 117.5 | 0 |
| p4 | 10/10 | 27.66 | 23.93-28.48 | 100.0% | 0 | 0 |
| p3 | 10/10 | 27.32 | 25.40-28.73 | 100.0% | 0 | 0 |
| p5 | 10/10 | 12.13 | 7.14-15.81 | 94.0% | 179.5 | 159 |

## Failure case quan sát được

Run `s4-p5-r9-278d0ab2`: P5 có 182 conflict, 26 thao tác abort; completion dữ liệu 92.5%. D hoàn thành 2/10 compaction. Retry có giới hạn không bảo đảm liveness/hoàn thành toàn bộ workload; oracle vẫn xác nhận dữ liệu của các thao tác accepted đúng.

## Diễn giải

- P0/P1 có thể đạt commit/s cao nhờ bỏ thao tác conflict; phải đọc cùng completion ratio và abort.
- P2/P5 retry có giới hạn: đúng các thao tác accepted không đảm bảo hoàn thành mọi intent.
- S4 đặt cả bốn worker trong p0, nên khóa partition P3 và khóa global P4 đều tuần tự hóa workload. Chênh lệch nhỏ không chứng minh lợi ích song song của P3.
- P5 ghi delta nhỏ nhưng đọc snapshot bằng cách mở và ghép các file active; compaction vẫn cần partition không đổi. Số file và retry có thể làm mất lợi thế giảm byte ghi.

## Giới hạn và mức kết luận

- Khoảng hoạt động chung nhỏ nhất của bốn worker: 0.993 giây. Workload này chưa đạt mục tiêu 30 giây của kế hoạch; không dùng kết quả để khẳng định hiệu năng workload dài hoặc throughput bão hòa.
- Activity window tính từ thao tác đầu đến cuối, bao gồm chờ queue/retry; không có nghĩa bốn transaction chạy CPU đồng thời.
- Maintenance overlap là giao nhau giữa khoảng bắt đầu-kết thúc logical operation của D và B/C, gồm queue/backoff; chưa đo thời gian rewrite vật lý chồng nhau.
- Compaction simulator luôn rewrite partition, kể cả chỉ có một file; không đại diện thuật toán chọn file hoặc no-op của hệ thực.
- p95 dùng nearest-rank và gồm cả thao tác abort; không phải p95 của riêng commit thành công. CSV worker có sample count.
- Write amplification chia byte Parquet đã ghi (gồm conflict và compaction) cho byte JSON logical intent accepted; không tính metadata I/O. Đây là proxy riêng của simulator.
- Không mô phỏng fsync/power loss, unknown outcome do mất kết nối hoặc lỗi lưu trữ production.
- Chưa có S3 chạy cùng phiên bản/code/config nên chưa định lượng được riêng chi phí compaction ở vùng nóng. Không so trực tiếp với kết quả S1 cũ.
- Môi trường một máy và hoạt động nền có thể ảnh hưởng thời gian; không có chứng minh khác biệt thống kê.

## Bàn giao

- s4-sorted.csv: tổng hợp policy, correctness, completion, latency, amplification, overlap.
- s4-runs-sorted.csv: 60 dòng cho từng run, không che các run sai.
- s4-worker-latencies.csv: p95 và số mẫu theo worker từng run.
- s4-summary.json: metric và xác minh độc lập, không chứa toàn bộ commit history lặp lại.
- Raw plan/receipts/events/metadata/Parquet giữ ở evidence_path; không commit thư mục results hoặc môi trường Python.
- Bản nén raw evidence được lưu riêng ở results/s4-reviewed/evidence.tar.gz, ngoài Git; giữ file này nếu cần di chuyển máy hoặc dọn /tmp.
- Chạy lại: xem S4-guide.md. Tổng hợp lại: `python scripts/summarize_s4.py <thư-mục-output> --output final`.
