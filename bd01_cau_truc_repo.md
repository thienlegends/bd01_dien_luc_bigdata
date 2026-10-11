# Cấu trúc repository `bd01_dien_luc` và đặc tả từng file

Nguồn chuẩn: đề cương mục **A.5** (repository), **A.4** (HDFS), **1.4–1.9** (bảng, job, DAG, web).
Cột **Trạng thái**: `CÓ` = file đã tồn tại, mới qua kiểm tra *tĩnh*, **chưa chạy thực tế**;
`KH` = kế hoạch, **chưa tạo** (giai đoạn sau). Không có hạng mục nào ở đây được coi là hoàn thành khi chưa có kết quả chạy.

## 1. Cây thư mục

```
bd01_dien_luc/
├── docker-compose.yml                 # CÓ  hạ tầng: HDFS, Spark Standalone, Kafka, Airflow (+ profile yarn, history)
├── README.md                          # CÓ  hướng dẫn dựng lại từ đầu
├── .gitignore                         # CÓ  chặn conf/.env, data/, spark-warehouse/, metastore_db/
├── conf/                              # theo A.5: spark-defaults.conf, hdfs-site.xml, .env.example
│   ├── .env.example                   # CÓ  biến môi trường mẫu (không có bí mật thật)
│   ├── spark-defaults.conf            # CÓ
│   ├── core-site.xml                  # CÓ  fs.defaultFS = hdfs://namenode:8020
│   ├── hdfs-site.xml                  # CÓ
│   ├── yarn-site.xml                  # CÓ  chỉ dùng với --profile yarn
│   ├── mapred-site.xml                # CÓ  chỉ dùng với --profile yarn
│   ├── docker/                        #     [mở rộng ngoài A.5] Dockerfile cho image tự build
│   │   ├── hadoop/{Dockerfile,entrypoint.sh}   # CÓ
│   │   ├── spark/Dockerfile                    # CÓ
│   │   └── airflow/Dockerfile                  # CÓ
│   └── scripts/                       #     [mở rộng ngoài A.5] script vận hành hạ tầng
│       ├── make_env.sh                # CÓ
│       ├── init_hdfs.sh               # CÓ
│       └── check_services.sh          # CÓ
├── spark_jobs/
│   ├── common/                        #     [mở rộng ngoài A.5] mã dùng chung (xem mục 3)
│   ├── ingest/                        # nạp bronze           — KH
│   ├── transform/                     # bronze→silver→gold   — KH
│   ├── streaming/                     # Structured Streaming — KH
│   └── ml/                            # train, evaluate, predict — KH
├── dags/
│   └── infra_smoke_test.py            # CÓ  DAG kiểm tra hạ tầng (không tính vào 3–4 DAG nghiệm thu)
├── webapp/{backend,frontend}/         # KH
├── notebooks/                         # KH  EDA
├── tests/
│   ├── infra/{static_checks.py,smoke_delta_hdfs.py,smoke_kafka_spark.py}   # CÓ
│   └── data_quality/                  # KH  kiểm thử chất lượng dữ liệu
└── docs/
    ├── REPO_STRUCTURE.md  TECH_DECISIONS.md  VERSION_COMPATIBILITY.md  MEMORY_BUDGET.md  AI_USAGE.md   # CÓ
    └── evidence/                      # nơi lưu kết quả chạy thật làm bằng chứng nghiệm thu
```

Thư mục **mở rộng ngoài A.5** (`conf/docker`, `conf/scripts`, `spark_jobs/common`, `tests/infra`, `docs/evidence`) đều nằm
*bên trong* các thư mục chuẩn hoặc là thư mục con; không có thư mục cấp một nào ngoài danh sách A.5. Nhóm nên xác nhận với giảng viên.

## 2. Đặc tả file hạ tầng (đã có)

| File | Nhiệm vụ | Đầu vào | Đầu ra | Phụ thuộc |
|---|---|---|---|---|
| `docker-compose.yml` | Định nghĩa dịch vụ, mạng `bd01_net`, volume, giới hạn RAM, healthcheck, thứ tự khởi động | `conf/.env` (qua `--env-file`), `conf/*.xml`, `conf/spark-defaults.conf`, `conf/docker/*` | container + volume; cổng chỉ bind `127.0.0.1` | tất cả file `conf/` |
| `conf/.env.example` | Mẫu biến môi trường: phiên bản ghim, URI HDFS, RAM, cổng, Airflow/Postgres/Kafka | — | được `make_env.sh` sao chép thành `conf/.env` | `static_checks.py` kiểm tra |
| `conf/spark-defaults.conf` | Cấu hình Spark (Standalone, client mode, Delta + Kafka packages, `fs.defaultFS`, warehouse/event log trên HDFS) | — | mount vào `/opt/spark/conf/` của `spark-client`, `airflow-*` | khớp `.env.example` (phiên bản) |
| `conf/core-site.xml` | `fs.defaultFS = hdfs://namenode:8020` — lớp phòng thủ chống ghi nhầm local | — | mount vào Hadoop và vào `HADOOP_CONF_DIR` của client | — |
| `conf/hdfs-site.xml` | replication=1, thư mục NameNode/DataNode, WebHDFS, quyền | — | như trên | — |
| `conf/yarn-site.xml`, `mapred-site.xml` | YARN 4 GB/4 vcore (chỉ profile `yarn`) | — | mount vào RM/NM | `docker-compose.yml` |
| `conf/docker/hadoop/Dockerfile` | Build image Hadoop 3.3.6 trên JRE 11, kiểm tra SHA-512 | `HADOOP_VERSION`, `JAVA_IMAGE_TAG` | image `bd01/hadoop` | `entrypoint.sh` |
| `conf/docker/hadoop/entrypoint.sh` | Vai trò `namenode/datanode/resourcemanager/nodemanager`; format NameNode **một lần** | tham số vai trò | tiến trình foreground | `hdfs-site.xml` |
| `conf/docker/spark/Dockerfile` | Image Spark 3.5.1 + `delta-spark` (pip, `--no-deps`) + pytest | `SPARK_VERSION`, `DELTA_VERSION` | image `bd01/spark` | — |
| `conf/docker/airflow/Dockerfile` | Image Airflow 2.9.3 + provider Spark 4.8.2 (theo constraints) + JRE 11 + bản phân phối Spark | build args | image `bd01/airflow` | image Spark, constraints Airflow |
| `conf/scripts/make_env.sh` | Sinh `conf/.env` với bí mật ngẫu nhiên | `conf/.env.example` | `conf/.env` (chmod 600) | python3 |
| `conf/scripts/init_hdfs.sh` | Tạo 7 thư mục A.4 + `/spark-logs`, `/user/spark/warehouse`, `/tmp`; **từ chối** `LAKEHOUSE_ROOT` không phải `hdfs://` | `LAKEHOUSE_ROOT` | cây thư mục HDFS | service `hdfs-init` |
| `conf/scripts/check_services.sh` | Kiểm tra sức khỏe toàn bộ hạ tầng, in PASS/FAIL | `conf/.env`, container đang chạy | bảng PASS/FAIL; exit 0/1 | docker compose |
| `tests/infra/static_checks.py` | Kiểm tra tĩnh: bí mật, biến, `hdfs://`, phiên bản khớp, cổng, XML, bash | các file `conf/`, compose | PASS/FAIL; exit 0/1 | docker CLI (tuỳ chọn) |
| `tests/infra/smoke_delta_hdfs.py` | Chứng minh Delta trên HDFS: ghi, MERGE, HISTORY, VERSION AS OF, CDF, OPTIMIZE (số file) | `HDFS_BASE_URI` | JSON `all_ok` | cụm Spark + HDFS |
| `tests/infra/smoke_kafka_spark.py` | Ghi rồi đọc 1 tin nhắn Kafka bằng Spark (V08) | `KAFKA_BOOTSTRAP_INTERNAL` | JSON `all_ok` | cụm Spark + Kafka |
| `dags/infra_smoke_test.py` | Airflow → SparkSubmitOperator → Spark → Delta → HDFS (V10/V11) | connection `spark_default` | task xanh + log JSON | `smoke_delta_hdfs.py` |
| `docs/*.md` | Quyết định, tương thích, RAM, khai báo AI, cấu trúc | — | — | — |

## 3. Đặc tả file pipeline (kế hoạch — CHƯA tạo)

Quy ước chung: mọi job đọc `LAKEHOUSE_ROOT` (có scheme `hdfs://`), **không** tự ghép đường dẫn local;
không dùng `.collect()`/`.toPandas()` trên DataFrame chưa lọc/tổng hợp xuống < 10.000 dòng (đề cương 1.5).

### 3.1 `spark_jobs/common/` (mở rộng)

| File | Nhiệm vụ | Đầu vào | Đầu ra | Phụ thuộc |
|---|---|---|---|---|
| `paths.py` | Dựng URI bảng từ `LAKEHOUSE_ROOT`; **ném lỗi** nếu không bắt đầu `hdfs://` | env `LAKEHOUSE_ROOT` | `table_path(tầng, tên)` | — |
| `spark_session.py` | Tạo SparkSession; kiểm tra `fs.defaultFS` là HDFS | tên ứng dụng | `SparkSession` | `paths.py` |
| `schemas.py` | `StructType` cho JSON Kafka, bronze, silver (đúng 1.4) | — | schema | — |
| `delta_utils.py` | Tạo bảng nếu chưa có (bật CDF khi cần), MERGE mẫu, OPTIMIZE đo trước/sau, chụp `DESCRIBE HISTORY` | đường dẫn bảng | số liệu, DataFrame | `paths.py` |

### 3.2 `spark_jobs/ingest/` — nạp bronze

| File | Nhiệm vụ | Đầu vào | Đầu ra | Phụ thuộc |
|---|---|---|---|---|
| `sinh_du_lieu_mo_phong.py` | Sinh 3 tình huống 1.2: 5% đến trễ 1–6 giờ, 1% hiệu chỉnh (cùng khóa, khác giá trị), 2% khách có bất thường cấy chủ ý; **ghi nhãn ground truth** | UCI CSV, seed, tỉ lệ | dữ liệu mô phỏng + bảng nhãn bất thường | — |
| `producer.py` | Phát lại lịch sử vào Kafka `meter_readings`, nén 1 ngày = 1 phút; tham số `--late-rate`, `--correction-rate` | dữ liệu mô phỏng, `KAFKA_BOOTSTRAP_*` | tin nhắn JSON | Kafka, `sinh_du_lieu_mo_phong.py` |
| `nap_lich_su_bronze.py` | **Nạp hàng loạt** lịch sử 2011–2014 vào bronze (batch) | UCI CSV | `bronze/doc_so_tho` | `schemas.py` |
| `nap_tham_chieu.py` | Nạp lịch ngày lễ VN + nhiệt độ Open-Meteo | API / file | bảng tham chiếu | `.env`: `WEATHER_*` |

### 3.3 `spark_jobs/streaming/`, `transform/`, `ml/`

| File | Nhiệm vụ (mục đề cương) | Đầu vào → Đầu ra | Phụ thuộc |
|---|---|---|---|
| `streaming/nap_doc_so.py` | Kafka → parse JSON → validate → bronze; `trigger(processingTime='30 seconds')`; checkpoint trên HDFS (1.5) | topic `meter_readings` → `bronze/doc_so_tho` + `checkpoints/nap_doc_so` | Kafka, `schemas.py` |
| `transform/bronze_to_silver.py` | Làm sạch, nội suy ≤ 4 bước, quarantine, `MERGE INTO` theo `(ma_khach_hang, thoi_diem_doc)` (1.4, 1.7) | bronze → `silver/doc_so`, `_quarantine/` | `delta_utils.py` |
| `transform/tao_gold.py` | Tính lại `gold.phu_tai_15p` chỉ cho ngày bị ảnh hưởng, đọc **CDF** của silver (1.5, 1.7) | silver (CDF) → `gold/phu_tai_15p` | silver bật CDF |
| `transform/bao_tri_delta.py` | OPTIMIZE (ZORDER) → VACUUM 168h → thống kê dung lượng HDFS (DAG 3) | các bảng → `gold/thong_ke_he_thong` | `delta_utils.py` |
| `ml/tao_dac_trung.py` | Window functions: lag, thống kê trượt, thời gian, chu kỳ, thời tiết (1.4) | silver + tham chiếu → `features/dac_trung_phu_tai` | `nap_tham_chieu.py` |
| `ml/huan_luyen_du_bao.py` | GBTRegressor + 2 baseline, split theo thời gian, tuning với TimeSeriesSplit tự cài (1.6) | features → `models/<phien_ban>` + `models/registry` | `tao_dac_trung.py` |
| `ml/danh_gia_du_bao.py` | Bảng MAPE/RMSE/MAE theo 3 nhóm horizon so với naive và LinearRegression (1.11) | mô hình + test → bảng so sánh | `huan_luyen_du_bao.py` |
| `ml/du_bao.py` | Dự báo 96 bước + khoảng tin cậy từ phân vị residual (1.4) | model đang dùng → `gold/du_bao_phu_tai` | registry |
| `ml/phat_hien_bat_thuong.py` | 3 tín hiệu: lệch residual (3σ, ≥ 8 bước), KMeans (k=5..8, silhouette), mất tín hiệu > 6 giờ (1.6) | silver + features → `gold/canh_bao_bat_thuong` | `du_bao.py` |
| `ml/danh_gia_bat_thuong.py` | Precision/Recall/F1 so với nhãn đã cấy (1.6, 1.11) | cảnh báo + nhãn → số liệu | `sinh_du_lieu_mo_phong.py` |

### 3.4 `dags/`, `webapp/`, `notebooks/`, `tests/data_quality/`

| File | Nhiệm vụ | Ghi chú |
|---|---|---|
| `dags/dl_pipeline_hang_gio.py` | DAG 1 (`0 * * * *`): Sensor → kiểm tra chất lượng → **BranchPythonOperator** (email nếu quarantine > 5% hoặc khách có dữ liệu < 80%) → silver → gold → đặc trưng → dự báo → cập nhật cache web; `retries=2`, `retry_delay=5'`, `sla=30'` (1.9) | cần chốt cách đọc số đo chất lượng (xem mục 4) |
| `dags/dl_huan_luyen_lai.py` | DAG 2 (`0 2 * * 0`): huấn luyện → đánh giá → promote hoặc giữ model cũ + cảnh báo | model registry = bảng Delta `models/registry` |
| `dags/dl_bao_tri.py` | DAG 3 (`0 3 * * *`): OPTIMIZE → VACUUM → thống kê HDFS | gọi `bao_tri_delta.py` |
| `dags/dl_backfill.py` | DAG 4 (không lịch): `start_date`, `end_date` qua `dag_run.conf`; demo 7 ngày | phải idempotent (câu hỏi phản biện 4) |
| `webapp/backend/` | FastAPI: `/api/phu-tai`, `/api/du-bao`, `/api/canh-bao`, `/api/khach-hang/{id}/tieu-thu`, `/api/canh-bao/{id}/ket-luan`, `/api/time-travel`, `/api/dag/{id}/trigger` (1.8) | đọc từ bảng gold/cache Parquet, không quét Delta lớn |
| `webapp/frontend/` | 5 màn hình: Tổng quan, Cảnh báo, Xử lý cảnh báo, Ảnh hưởng dữ liệu trễ, Quản trị | A.3: React/Vue hoặc Jinja2 + Chart.js (Streamlit bị trừ 20%) |
| `notebooks/` | EDA dữ liệu UCI, thống kê chất lượng | tuần 2 |
| `tests/data_quality/` | Kiểm thử luật silver, nhất quán gold, quét mã cấm `.collect()`/`.toPandas()` | — |

## 4. Điểm đề cương chưa rõ / chưa đầy đủ (cần quyết định, KHÔNG tự đoán)

| # | Vấn đề | Đề xuất tạm | Cần ai quyết |
|---|---|---|---|
| G1 | Tên bảng bronze viết **hai kiểu**: `doc_so_thô` (sơ đồ 1.3) và `doc_so_tho` (đặc tả 1.4) | dùng `doc_so_tho` (không dấu), ghi chú trong báo cáo | nhóm |
| G2 | Phát lại Kafka 1 ngày = 1 phút ⇒ 3 năm lịch sử ≈ 18 giờ, không thể dùng để nạp dữ liệu huấn luyện | thêm `nap_lich_su_bronze.py` (batch) cho 2011–2013 | nhóm + giảng viên |
| G3 | Bảng `models/registry` (DAG 2), `gold.thong_ke_he_thong` (DAG 3), bảng tham chiếu (lễ, thời tiết) và **nhãn bất thường đã cấy** không có trong mục 1.4 | tạo thêm, ghi rõ là bảng bổ sung | nhóm |
| G4 | Dữ liệu UCI là của Bồ Đào Nha, đề cương yêu cầu lịch lễ VN + nhiệt độ Open-Meteo: toạ độ nào? | để trống `WEATHER_LATITUDE/LONGITUDE` có chủ ý | giảng viên |
| G5 | Web ghi ngược vào Delta trên HDFS bằng cách nào (A.6.4)? DuckDB không đọc `hdfs://` | cache Parquet cho đọc; ghi qua Spark (cách cụ thể: giai đoạn web) | nhóm — xem `TECH_DECISIONS.md` Q4 |
| G6 | DAG 1: `PythonOperator` kiểm tra chất lượng cần số liệu từ Delta trên HDFS nhưng Airflow không có thư viện đọc Delta | job Spark ghi số đo ra bảng/JSON nhỏ, DAG đọc qua WebHDFS | nhóm |
| G7 | Bảng A.3 ghi "HDFS + YARN" nhưng cũng cho phép Spark Standalone | Standalone mặc định, YARN là profile tuỳ chọn | giảng viên (`TECH_DECISIONS.md` Q1) |
| G8 | `C.2` ghi `spark.executor.instances 2` (cho YARN); Standalone không dùng khóa này | dùng `spark.cores.max=4` + `spark.executor.cores=2` | — (đã xử lý trong `spark-defaults.conf`) |
| G9 | Thư mục `_quarantine` bắt đầu bằng dấu `_` — Spark/Hadoop coi tên bắt đầu `_`/`.` là file ẩn khi liệt kê | ghi và đọc Delta ở `_quarantine/<bảng>` rồi kiểm tra MERGE/OPTIMIZE hoạt động; nếu lỗi, báo giảng viên | cần thử (xem V14 trong `VERSION_COMPATIBILITY.md`) |
