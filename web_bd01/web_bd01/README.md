# BD01 Điện lực — Web giám sát, dự báo và cảnh báo bất thường

Ứng dụng web **FastAPI + Jinja2 + Chart.js + DuckDB** cho đề tài Big Data điện lực. Web không truy vấn trực tiếp các bảng Delta lớn; Spark xuất các bảng cần hiển thị thành Parquet cache để API đọc nhanh bằng DuckDB.

## 1. Các chức năng

- **Tổng quan:** phụ tải thực tế 15 phút, dự báo, MAPE, số cảnh báo và tình trạng cache/Airflow.
- **Cảnh báo:** lọc theo loại, trạng thái, điểm bất thường; xem bằng chứng.
- **Xử lý cảnh báo:** xem chuỗi tiêu thụ của khách hàng, ghi kết luận vào SQLite để giao diện phản hồi ngay, sau đó đồng bộ nền vào `silver/xac_nhan_canh_bao` và MERGE trạng thái vào `gold/canh_bao_bat_thuong`.
- **Ảnh hưởng dữ liệu đến trễ:** so sánh dự báo theo `silver_version` và dữ liệu thực tế.
- **Quản trị:** xem lịch sử DAG, kích hoạt pipeline/backfill và đồng bộ cache.
- **Health check:** `GET /healthz`; trạng thái tích hợp: `GET /api/tinh-trang`; tài liệu API: `/docs`.

## 2. Chạy web nhanh bằng dữ liệu demo

Python 3.10+ được khuyến nghị. Trên máy chạy web:

```bash
cd web_bd01
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 tao_du_lieu_demo.py
GHI_DELTA_CMD=off uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Mở `http://localhost:8000`. Nếu truy cập từ máy khác trong mạng, dùng `http://IP_MAY_CHAY_WEB:8000`.

Dữ liệu demo là dữ liệu **giả lập để kiểm tra giao diện**, không phải kết quả thực nghiệm của UCI hoặc mô hình dự báo thật. Web sẽ hiển thị nhãn “ĐANG DÙNG DỮ LIỆU DEMO”. Khi đồng bộ cache thật thành công, script sẽ xóa nhãn này.

## 3. Kết nối với pipeline Big Data thật

### 3.1. Quy ước đường dẫn Delta

Mặc định, ứng dụng kỳ vọng các bảng Delta tại:

```text
hdfs://master:9000/lakehouse/bd01_dien_luc/
├── silver/doc_so
├── gold/phu_tai_15p
├── gold/du_bao_phu_tai
├── gold/canh_bao_bat_thuong
└── silver/xac_nhan_canh_bao    # được tạo khi ghi kết luận lần đầu
```

Đường dẫn trên phải khớp với schema và vị trí bảng mà pipeline của nhóm thực sự tạo. Nếu khác, sửa biến `LAKEHOUSE` hoặc đối số `--goc`.

### 3.2. Xuất cache từ Spark

Chạy trên máy có `spark-submit` và truy cập được HDFS/Delta:

```bash
spark-submit \
  --master spark://master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  spark_jobs/cap_nhat_cache_web.py \
  --goc hdfs://master:9000/lakehouse/bd01_dien_luc
```

`delta-spark_2.12:3.2.0` phù hợp với Spark 3.5.x. Nếu cụm đang dùng phiên bản Spark khác, cần chọn phiên bản Delta tương thích thay vì sao chép nguyên cấu hình.

Job xuất các thư mục Parquet vào `.../cache_web/`: `phu_tai_15p`, `du_bao_phu_tai`, `canh_bao_bat_thuong`, `tieu_thu`, `du_lieu_tre`. Tên/cột trong bảng Delta nguồn phải khớp với các phép `select` trong `spark_jobs/cap_nhat_cache_web.py`.

### 3.3. Đồng bộ cache về máy web

Trên máy chạy web cần có Hadoop client (`hdfs`) và cấu hình truy cập HDFS:

```bash
bash scripts/dong_bo_cache.sh
```

Nếu cần đổi đường dẫn, đặt `HDFS_CACHE_DIR` và `CACHE_DIR` trước khi chạy. Mặc định HDFS là `/lakehouse/bd01_dien_luc/cache_web` và cache local là `web_bd01/cache`.

Trong Airflow, đặt job `cap_nhat_cache_web` sau các task cập nhật Silver/Gold, rồi gọi script đồng bộ cache. Nếu web chạy trên máy riêng không có `hdfs`, nên dùng task triển khai trên máy có Hadoop client để chuyển cache sang máy web.

## 4. Ghi kết luận cảnh báo ngược vào Delta

Mặc định web gọi `spark-submit` với `SPARK_MASTER`, `DELTA_PKG` và `LAKEHOUSE`. Cấu hình ví dụ:

```bash
export SPARK_MASTER='spark://master:7077'
export DELTA_PKG='io.delta:delta-spark_2.12:3.2.0'
export LAKEHOUSE='hdfs://master:9000/lakehouse/bd01_dien_luc'
```

Để chỉ thử giao diện và không ghi Delta:

```bash
GHI_DELTA_CMD=off uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Khi tắt đồng bộ, kết luận chỉ được lưu cục bộ trong `data/xac_nhan.sqlite`; đây không phải là ghi thành công vào Delta. Khi bật, máy chạy web phải có `spark-submit`, Java và quyền đọc/ghi HDFS. Bảng `gold/canh_bao_bat_thuong` cần tồn tại trước khi MERGE.

## 5. Cấu hình môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `CACHE_DIR` | `./cache` trong thư mục dự án | Cache Parquet local |
| `DATA_DIR` | `./data` trong thư mục dự án | SQLite cục bộ |
| `AIRFLOW_URL` | `http://master:8081` | Airflow webserver |
| `AIRFLOW_API` | `/api/v1` | Prefix REST API Airflow 2.x |
| `AIRFLOW_USER` / `AIRFLOW_PASS` | `admin` / `admin` | Tài khoản API; hãy đổi theo môi trường thực tế |
| `SPARK_MASTER` | `spark://master:7077` | Spark standalone master |
| `DELTA_PKG` | `io.delta:delta-spark_2.12:3.2.0` | Gói Delta tương thích Spark |
| `LAKEHOUSE` | `hdfs://master:9000/lakehouse/bd01_dien_luc` | Gốc lakehouse |
| `GHI_DELTA_CMD` | không đặt | Đặt `off` để tắt ghi Delta, hoặc truyền lệnh tùy chỉnh với `{json}` làm chỗ chèn payload |

Airflow cần bật cơ chế xác thực REST API phù hợp. Ứng dụng hiện giả định Airflow 2.x REST API; Airflow 3 có thể cần điều chỉnh endpoint/xác thực.

## 6. Các API chính

| Phương thức | Endpoint | Mục đích |
|---|---|---|
| GET | `/api/phu-tai` | Chuỗi phụ tải thực tế |
| GET | `/api/du-bao` | Lần dự báo mới nhất |
| GET | `/api/kpi` | MAPE và KPI cảnh báo |
| GET | `/api/canh-bao` | Danh sách/lọc cảnh báo |
| GET | `/api/canh-bao/{id}` | Chi tiết cảnh báo |
| POST | `/api/canh-bao/{id}/ketluan` | Ghi kết luận và đồng bộ Delta nền |
| GET | `/api/khach-hang/{id}/tieu-thu` | Lịch sử tiêu thụ khách hàng |
| GET | `/api/timetravel?ngay=YYYY-MM-DD` | So sánh dự báo theo phiên bản Silver |
| GET | `/api/tinh-trang` | Cache, chế độ dữ liệu và trạng thái Airflow |
| POST | `/api/dag/{dag_id}/trigger` | Kích hoạt DAG trong danh sách cho phép |
| POST | `/api/cache/dong-bo` | Đồng bộ cache từ HDFS |

## 7. Trước khi demo/nghiệm thu

- [ ] Web đang dùng cache thật, không phải cache demo.
- [ ] Tên và schema bảng Delta khớp với job xuất cache.
- [ ] API phụ tải, dự báo, cảnh báo và tiêu thụ trả dữ liệu.
- [ ] Kafka producer/consumer và Spark Streaming được chứng minh ở pipeline; web không giả định rằng chỉ mở web là đã có streaming trực tiếp.
- [ ] Ghi kết luận cảnh báo được kiểm tra cả trạng thái SQLite lẫn Delta.
- [ ] Airflow DAG ID trong `app/airflow_client.py` trùng với DAG đã triển khai.
- [ ] Đổi mật khẩu mặc định và chỉ mở web trong mạng lab đáng tin cậy; endpoint quản trị hiện chưa có đăng nhập/phân quyền.
- [ ] Khi viết báo cáo, phân biệt rõ số liệu demo với kết quả chạy trên UCI và kết quả mô hình thực tế.
