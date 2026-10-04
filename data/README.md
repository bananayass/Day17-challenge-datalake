# Dữ liệu ban đầu dùng chung

`initial-data.json` là mảng JSON gồm **8.000 dòng**, chia đều cho 8 partition
`p0` đến `p7`. Dữ liệu tổng hợp phục vụ kiểm tra tính toàn vẹn khi ghi đồng thời,
theo `experiment-scenarios.md`.

Mỗi partition có 1.000 bộ đếm, khóa từ 0 đến 999. Tất cả bắt đầu bằng 0 để
nhóm dễ tính số lần tăng được ghi thành công. Chưa có event ở trạng thái ban đầu;
worker A sẽ thêm các event trong thí nghiệm.

```json
{
  "partition_id": "p0",
  "row_kind": "counter",
  "row_id": "counter-p0-0000",
  "counter_value": 0,
  "event_payload": null
}
```

## Quy ước cho cả nhóm

- `row_id` duy nhất trên toàn bộ dữ liệu. Khóa số 10 trong p0 tương ứng
  `counter-p0-0010`. Giữ nguyên ID giữa các lần chạy.
- `counter_value` dùng kiểu số nguyên 64 bit (`long`). Worker B/C tăng giá trị
  trong giao dịch; không đọc ra client rồi ghi lại giá trị tuyệt đối.
- `event_payload` dùng kiểu `string`, cho phép null. Nếu event cần nhiều thuộc
  tính, lưu một chuỗi JSON đã serialize trong cột này. Event dùng `row_kind`
  là `event`, `counter_value` là null và ID riêng có tiền tố `event-`.
- Nạp lại dữ liệu gốc vào bảng mới trước mỗi lần chạy; không sửa file dùng chung.
- `initial-data.manifest.json` ghi schema, số dòng và SHA-256 để đối chiếu
  dữ liệu giữa các thành viên. Manifest không phải dữ liệu để nạp vào bảng.

## Nạp dữ liệu bằng Spark

Khai báo schema rõ ràng vì toàn bộ `event_payload` ban đầu là null.

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("partition_id", StringType(), False),
    StructField("row_kind", StringType(), False),
    StructField("row_id", StringType(), False),
    StructField("counter_value", LongType(), True),
    StructField("event_payload", StringType(), True),
])
initial_df = spark.read.schema(schema).option("multiLine", "true").json(
    "data/initial-data.json"
)
```

Sau đó ghi vào bảng Delta Lake hoặc Iceberg của nhóm, partition theo
`partition_id`. Chỉ đọc đúng file dữ liệu, tránh đọc cả thư mục chứa manifest.

## Bố trí file cho thí nghiệm

Mục tiêu là khoảng 10 file dữ liệu nhỏ mỗi partition, mỗi file chứa 100 khóa
liên tiếp: 0-99, 100-199, ..., 900-999. Như vậy khóa 0-9 của B và 10-19 của C
có thể nằm chung file trong S2. **Một file JSON không tự tạo ra bố trí file
Delta/Iceberg này**: nhóm cần điều khiển cách ghi và kiểm tra file vật lý sau
khi nạp. Lưu lại ánh xạ khóa tới file làm bằng chứng.

8.000 dòng là quy mô khởi đầu để kiểm tra logic. Cần hiệu chỉnh khối lượng
thao tác để các worker thực sự chạy đồng thời ít nhất 30 giây như kế hoạch;
chưa thể coi đây là bộ benchmark hiệu năng đã được hiệu chỉnh.

## Tạo lại dữ liệu

Chạy từ thư mục gốc repo, không cần thư viện ngoài:

```powershell
python scripts/generate_initial_data.py
```

Script tạo lại file dữ liệu và manifest với nội dung cố định, không dùng số
ngẫu nhiên hoặc thời gian hiện tại. Bộ dữ liệu này chỉ là trạng thái ban đầu;
danh sách thao tác và event đầu vào phải được chuẩn bị riêng trước thí nghiệm.
