# Các quyết định kỹ thuật giai đoạn hạ tầng và cách kiểm chứng

Mỗi quyết định có: lý do, phương án đã loại, **điều kiện để quyết định bị lật lại**, và cách kiểm chứng.
Mã V.. tham chiếu `VERSION_COMPATIBILITY.md` mục 3.

| Mã | Quyết định | Lý do | Phương án đã loại | Kiểm chứng |
|---|---|---|---|---|
| D1 | Ghim Hadoop 3.3.6 · Spark 3.5.1 · Delta 3.1.0 · Kafka 3.6.x (Confluent 7.6.14) · Airflow 2.9.3 · Java 11 | Khớp bảng A.3. Spark 3.5.1 trùng `pyspark==3.5.1` trong constraints Airflow 2.9.3. `apache/kafka` chính thức chỉ có từ 3.7 | Spark 3.5.3+ (phải ghi đè constraints); Kafka 3.7+ (vượt yêu cầu 3.6.x) | V01–V09 |
| D2 | Spark **Standalone** mặc định; YARN daemon là profile tuỳ chọn | A.3 cho phép "YARN hoặc Standalone"; tiết kiệm RAM; cluster mode không có cho PySpark trên Standalone nên dùng client mode | Spark-on-YARN ngay (cần Python + jar Spark trong NodeManager, thêm ~1,5 GB RAM). **Cần xác nhận với giảng viên**: bảng A.3 ghi "HDFS + YARN" | V05, V13 |
| D3 | Delta chỉ ghi bằng **URI đầy đủ** `hdfs://namenode:8020/lakehouse/bd01_dien_luc/...`; `fs.defaultFS`, `warehouse.dir`, `eventLog.dir` đều HDFS; smoke test và `static_checks.py` chặn `file:` | A.6.3: không được để Delta ở local. Đường dẫn không scheme rơi về `file:` khi thiếu cấu hình là lỗi âm thầm | Tin vào `defaultFS` mà không có rào chắn | V07 + `static_checks.py` S02 |
| D4 | Bảng Delta truy cập **theo đường dẫn**, chưa dựng Hive Metastore | Catalog mặc định của Spark là Derby cục bộ (`metastore_db`, `spark-warehouse`) = local filesystem. Đề cương dùng tên `silver.doc_so` nhưng không bắt buộc catalog | Hive Metastore (thêm Postgres + 1 container ~1 GB) | **Câu hỏi mở Q2**; nếu cần tên bảng, nâng cấp sau |
| D5 | Kafka KRaft 1 node | Bớt ZooKeeper; KRaft dùng được từ Kafka 3.3 | ZooKeeper + Kafka (2 container) | V04 |
| D6 | Tự build image Hadoop; Airflow COPY bản phân phối Spark + JRE | Cấu hình hoàn toàn bằng XML trong `conf/` (đúng cấu trúc A.5); `spark-submit` trong Airflow cùng bản/Java/Python với cluster | `apache/hadoop` (cơ chế cấu hình env chưa kiểm chứng); cài `pyspark` pip trong Airflow (thiếu `sbin`, Java) | V01, V09 |
| D7 | Airflow LocalExecutor + Postgres 16 | Ít container; Postgres 16 nằm trong dải hỗ trợ 12–16 | CeleryExecutor + Redis | V09 |
| D8 | **Cả** Delta (`delta-spark_2.12:3.1.0`) **và** connector Kafka (`spark-sql-kafka-0-10_2.12:3.5.1`) khai báo trong `spark.jars.packages` + volume Ivy; job **không** truyền `--packages` | Giống tinh thần lệnh mẫu C.2 nhưng tránh cạm bẫy: `--packages` trên dòng lệnh **ghi đè** `spark.jars.packages`, job quên liệt kê lại Delta sẽ mất extension. Không phải nhúng jar | Nhúng jar vào image (offline tốt hơn nhưng phải tự giải phụ thuộc bắc cầu); chỉ khai Delta rồi truyền Kafka ở dòng lệnh (dễ quên) | V06, V08 |
| D9 | HDFS 1 DataNode, replication 1; mọi client dùng `HADOOP_USER_NAME=hadoop` (superuser) | Pseudo-distributed hợp lệ (A.3). Tránh lỗi quyền giữa uid của `spark`, `airflow`, `hadoop`. Executor nhận `spark.hadoop.dfs.replication=1` | `dfs.permissions.enabled=false` | V02, V03 |
| D10 | Spark session timezone **UTC**; Airflow `Asia/Ho_Chi_Minh` | Dữ liệu UCI có ngày đổi giờ (23h/25h); UTC tránh nhập nhằng. Lịch Airflow (vd 2h sáng CN) theo giờ Việt Nam | Cùng một timezone cho cả hai | Xác nhận ở giai đoạn dữ liệu |
| D11 | Web dùng **cache Parquet** trong volume `web_cache`; cache được ghi bởi driver (dữ liệu gold đã tổng hợp < 10.000 dòng) | 1.8: web không query thẳng Delta lớn; DuckDB không đọc `hdfs://`. Cache **không phải** nơi lưu Delta | Ghi cache từ executor (sẽ rơi vào đĩa local của từng worker) | Giai đoạn web |
| D12 | Cổng chỉ bind `127.0.0.1`; `conf/.env` do script sinh, không commit | Không mở dịch vụ ra mạng; không có bí mật thật trong repo | Cổng mở mọi interface | `static_checks.py` S01, S04c |
| D13 | Job streaming dùng `--total-executor-cores 2`; batch dùng 2 core còn lại | Standalone mặc định cho một ứng dụng chiếm hết core ⇒ batch đói tài nguyên | Để mặc định 4 core | V12 |

## Câu hỏi mở cần người dùng/giảng viên quyết

- **Q1.** Có chấp nhận Spark Standalone (đề cương cho phép) hay thầy/cô muốn thấy Spark-on-YARN? (D2)
- **Q2.** Có cần gọi bảng theo tên `silver.doc_so` trong demo (cần Hive Metastore) hay đường dẫn `delta.\`hdfs://…\`` là đủ? (D4)
- **Q3.** Dữ liệu UCI là của Bồ Đào Nha, đề cương lại yêu cầu lịch lễ Việt Nam + nhiệt độ Open-Meteo. Dùng toạ độ nào cho thời tiết? (`WEATHER_LATITUDE/LONGITUDE` đang để trống có chủ ý)
- **Q4.** Web ghi ngược vào Delta trên HDFS (A.6.4) bằng cách nào: SparkSession nhẹ trong backend, hay gửi job qua Airflow/`spark-submit`? Quyết định ở giai đoạn web; ảnh hưởng RAM và độ trễ < 3 giây.
