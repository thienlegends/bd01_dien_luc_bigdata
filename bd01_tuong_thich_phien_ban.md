# Tương thích phiên bản — những gì đã xác minh và những gì CHƯA

> Nguyên tắc: **không giả định** các phiên bản "tự động tương thích". Mỗi cặp bên dưới ghi rõ
> (a) tài liệu/metadata nói gì, (b) còn rủi ro gì, (c) lệnh kiểm chứng khi chạy thực tế.
> Cột "Trạng thái": **Đã đối chiếu tài liệu** = chỉ mới đọc tài liệu/metadata ngày thiết kế (2026-10-11);
> **Chưa chạy** = chưa có kết quả chạy thực tế nào. Hiện tại **chưa có hạng mục nào được kiểm chứng bằng chạy thực tế**.

## 1. Phiên bản được ghim

| Thành phần | Phiên bản | Yêu cầu đề cương (A.3) | Ghi chú |
|---|---|---|---|
| Apache Hadoop (HDFS) | 3.3.6 | ≥ 3.3.x | tự build từ tarball `archive.apache.org` |
| Apache Spark | 3.5.1 (Scala 2.12, Java 11, Python 3.10) | ≥ 3.5.x | image `apache/spark:3.5.1-scala2.12-java11-python3-ubuntu` |
| Delta Lake | 3.1.0 (`io.delta:delta-spark_2.12:3.1.0`) | 3.1.x | nạp bằng `spark.jars.packages`; `delta-spark==3.1.0` (pip) cho module Python |
| Apache Kafka | 3.6.x qua `confluentinc/cp-kafka:7.6.14`, KRaft | ≥ 3.6.x | package Spark: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` |
| Apache Airflow | 2.9.3, Python 3.10, LocalExecutor | 2.9.x | provider `apache-airflow-providers-apache-spark==4.8.2` |
| PostgreSQL (metadata Airflow) | 16 | — | |
| JRE | Temurin 11 (Hadoop, Spark, Airflow dùng chung) | — | |

## 2. Ma trận từng cặp

| # | Cặp | Bằng chứng đã đọc | Rủi ro còn lại | Trạng thái | Kiểm chứng |
|---|---|---|---|---|---|
| C1 | Delta 3.1.x ↔ Spark 3.5.x | Bảng phát hành của Delta (docs.delta.io/latest/releases.html): `3.1.x → Spark 3.5.x`. `delta-spark 3.1.0` yêu cầu pip `pyspark>=3.5.0,<3.6.0` (setup.py, tag v3.1.0) | Bảng chỉ ghi dòng `3.5.x`, không nêu bản vá cụ thể; ta chạy Spark 3.5.1 và chưa có tài liệu nào xác nhận riêng cặp Delta 3.1.0 ↔ Spark 3.5.1 | Đã đối chiếu tài liệu · Chưa chạy | V06, V07 |
| C2 | Delta ↔ HDFS | Tài liệu lưu trữ của Delta: hỗ trợ HDFS "built-in … full transactional guarantees", không cần cấu hình thêm; LogStore mặc định cần rename nguyên tử không ghi đè | Tài liệu không nêu tên LogStore cho `hdfs://`; phải xác nhận bằng chạy MERGE đồng thời | Đã đối chiếu tài liệu · Chưa chạy | V07 |
| C3 | Spark 3.5.1 ↔ Hadoop | `pom.xml` của Spark (v3.5.1 và v3.5.3): `hadoop.version = 3.3.4` (client đi kèm bản `spark-3.5.1-bin-hadoop3`). Server ta chạy là 3.3.6 | Client 3.3.4 ↔ server 3.3.6 cùng dòng 3.3.x; kỳ vọng tương thích RPC nhưng **chưa có tài liệu trích dẫn trong phiên này** | Chưa chạy | V07 |
| C4 | Spark 3.5.1 ↔ Kafka broker 3.6 | `pom.xml`: `kafka.version = 3.4.1` (kafka-clients mà `spark-sql-kafka-0-10` 3.5.1 kéo về), `commons-pool2 = 2.11.1`. Client 3.4.1 < broker 3.6 (phiên bản này ghi theo lần đọc `pom.xml` trước; lần rà soát sau, `repo1.maven.org` từ chối truy cập tự động nên **không đọc lại được** — hãy xác nhận bằng `ls /opt/ivy/jars \| grep kafka-clients`) | Cần kiểm chứng với job thật. Cả Delta và Kafka đều nằm trong `spark.jars.packages` (tránh `--packages` ghi đè) | Đã đối chiếu metadata · Chưa chạy | V08 |
| C5 | Kafka 3.6 ↔ Confluent 7.6 | Bảng Interoperability của Confluent: `7.6.x ↔ Apache Kafka 3.6.x`. Biến môi trường KRaft lấy từ `cp-all-in-one-kraft` tag `7.6.0-post` | Dùng 7.6.14 (bản vá mới hơn 7.6.0) — biến môi trường có thể khác nhẹ | Đã đối chiếu tài liệu · Chưa chạy | V04 |
| C6 | Airflow 2.9.3 ↔ Python/Postgres | Tài liệu cài đặt 2.9.3: Python 3.8–3.12; PostgreSQL 12–16 | — | Đã đối chiếu tài liệu · Chưa chạy | V09 |
| C7 | Airflow 2.9.3 ↔ Spark provider ↔ PySpark | `constraints-2.9.3/constraints-3.10.txt`: `pyspark==3.5.1`, `apache-airflow-providers-apache-spark==4.8.2`, `py4j==0.10.9.7`. **Đây là lý do chọn Spark 3.5.1** (khỏi ghi đè constraints) | Ta COPY thêm bản phân phối Spark 3.5.1 vào image Airflow để `spark-submit` trùng cluster | Đã đối chiếu metadata · Chưa chạy | V09, V10 |
| C8 | Python driver ↔ executor | PySpark yêu cầu cùng phiên bản minor. Image Spark dựa `eclipse-temurin:*-jammy` (Ubuntu 22.04 → Python 3.10); Airflow image `python3.10` | Số phiên bản Python của Ubuntu 22.04 là kiến thức nền, chưa kiểm bằng lệnh | Chưa chạy | V05, V11 |
| C9 | Java ↔ Hadoop 3.3.x | Wiki "Hadoop Java Versions": 3.3.x hỗ trợ runtime Java 8 và 11; trang không nói về Java 17 ⇒ **không dùng 17** | Java 11 với Spark 3.5/Delta 3.1: kiến thức nền, chưa trích dẫn trong phiên này | Đã đối chiếu một phần · Chưa chạy | V01, V05 |
| C10 | Airflow ↔ JRE copy từ Ubuntu | — | JRE `temurin:11-jre-jammy` copy sang Debian bookworm: kỳ vọng chạy (glibc mới hơn) nhưng chưa thử | Chưa chạy | V09 |
| C11 | Image tồn tại | Docker Hub API (ngày thiết kế): `apache/spark:3.5.1-scala2.12-java11-python3-ubuntu`, `confluentinc/cp-kafka:7.6.14`, `eclipse-temurin:11-jre-jammy`, `apache/airflow:2.9.3-python3.10`, `postgres:16`, `axllent/mailpit:v1.31` đều có tag | Tarball Hadoop 3.3.6 trên `archive.apache.org` chưa tải được để xác nhận (trang chặn robot) | Đã đối chiếu · Chưa chạy | V01 |

## 3. Cấu hình CẦN kiểm chứng (không giả định)

| Mã | Nội dung | Lệnh / dấu hiệu đạt | Lưu bằng chứng vào |
|---|---|---|---|
| V01 | Image Hadoop build được; SHA-512 tarball khớp; Java 11 | `docker compose build namenode` thành công; `docker compose run --rm hdfs-init hdfs version` in `Hadoop 3.3.6` | `docs/evidence/v01_hadoop_version.txt` |
| V02 | NameNode format lần đầu, DataNode đăng ký | `docker compose exec namenode hdfs dfsadmin -report` có `Live datanodes (1)` | `v02_dfsadmin_report.txt` |
| V03 | `hdfs-init` tạo đủ 7 thư mục chuẩn; client từ container Spark/Airflow ghi được (`HADOOP_USER_NAME=hadoop`) | `hdfs dfs -ls hdfs://namenode:8020/lakehouse/bd01_dien_luc` thấy 7 thư mục; `spark-client` chạy `hdfs`-ghi thử qua smoke test | `v03_hdfs_tree.txt` |
| V04 | Kafka 3.6.x KRaft khởi động; tạo topic; produce/consume | `kafka-topics --version` (kỳ vọng `3.6.x`); `check_services.sh` mục 3 | `v04_kafka.txt` |
| V05 | Image Spark build; master + 2 worker ALIVE; Python 3.10 | `curl 127.0.0.1:8090/json/` → `aliveworkers: 2`; `python3 --version` | `v05_spark_cluster.txt` |
| V06 | Jar Delta nạp được qua Ivy; extension khởi tạo với Spark 3.5.1; `import delta` chạy | log spark-submit có `io.delta#delta-spark_2.12;3.1.0` resolved; `python3 -c "import pyspark, delta"` | `v06_delta_load.txt` |
| V07 | Delta trên HDFS: ghi, MERGE, HISTORY, VERSION AS OF, CDF, OPTIMIZE; vị trí `hdfs://` | `spark-submit /opt/bd01/tests/infra/smoke_delta_hdfs.py` → `all_ok: true`; sau đó `hdfs dfs -du -h /tmp/bd01_smoke` (dùng `--keep` để giữ lại) | `docs/evidence/smoke_delta_hdfs.json` |
| V08 | Kafka source Spark 3.5.1 ↔ broker 3.6 (gói nằm trong `spark.jars.packages`, không dùng `--packages`) | `tests/infra/smoke_kafka_spark.py` → `all_ok: true`; không lỗi `NoSuchMethodError`/`ClassNotFound`; `ls /opt/ivy/jars \| grep kafka-clients` ghi phiên bản thực tế | `v08_kafka_source.txt` |
| V09 | Image Airflow build đúng constraints; JRE copy chạy; `spark-submit` trong Airflow cùng 3.5.1 | `docker compose exec airflow-scheduler spark-submit --version`; `pip check` | `v09_airflow_image.txt` |
| V10 | Connection `spark_default` (JSON, host `spark://spark-master`) được provider 4.8.2 dùng đúng làm master URL | `airflow connections get spark_default`; log task có `--master spark://spark-master:7077` | `v10_spark_conn.txt` |
| V11 | DAG `infra_smoke_test` chạy xanh: executor kết nối ngược được driver trong `airflow-scheduler`; Python minor trùng | trạng thái `success` trên UI + log JSON `all_ok` | ảnh chụp UI + `v11_dag_smoke.txt` |
| V12 | Bộ nhớ: không OOM khi streaming (2 core) và batch (2 core) chạy đồng thời | `docker stats --no-stream`; `docker inspect -f '{{.State.OOMKilled}}'` cho mọi container = `false` | `v12_docker_stats.txt` |
| V13 | (tuỳ chọn) YARN daemon lên | `--profile yarn`: `yarn node -list` có 1 node RUNNING | `v13_yarn.txt` |
| V14 | Delta đặt dưới thư mục bắt đầu bằng `_` (`/lakehouse/bd01_dien_luc/_quarantine/<bảng>`): ghi, đọc, MERGE, OPTIMIZE đều chạy được (Spark/Hadoop coi tên bắt đầu `_` là file ẩn khi liệt kê) | ghi 1 bảng Delta thử vào `_quarantine/_thu`, `spark.read.format("delta")`, `OPTIMIZE`, rồi xóa. Nếu lỗi: báo giảng viên, **không** đổi tên thư mục tự ý | `v14_quarantine_path.txt` |

## 4. Giả định chưa kiểm — cần để mắt khi gặp lỗi

- `/opt/spark/work-dir` có sẵn và ghi được trong image `apache/spark` (đã `mkdir`/`chown` trong Dockerfile để phòng).
- Entrypoint của image Spark chuyển tiếp lệnh tuỳ ý (`spark-class …`): **đã đọc** `entrypoint.sh.template` của `apache/spark-docker` — nhánh `*)` chạy `exec "$@"` ở chế độ pass-through, không đổi người dùng. Vẫn cần xác nhận khi chạy thật (V05).
- Image `apache/spark:3.5.1-scala2.12-java11-python3-ubuntu` dựa trên Ubuntu 22.04 (Python 3.10): **đã đọc** Dockerfile của biến thể java17 (`FROM eclipse-temurin:17-jammy`, `apt-get install python3`); biến thể java11 được suy ra tương tự, **chưa đọc trực tiếp** — kiểm bằng `python3 --version` (V05).
- Bind-mount một file đè lên `/opt/spark/conf/spark-defaults.conf` hoạt động trong image `apache/spark`.
- `spark.databricks.delta.autoCompact.enabled` có hiệu lực ở Delta 3.1.0 hay không (đang để **tắt**; chỉ bật sau khi đo hiện tượng small files).
- Định dạng file `.sha512` của Hadoop (một dòng hay nhiều dòng); lệnh trích hash đã viết để chịu được cả hai.
- Tên khoá `deploy-mode` trong `extra` của connection Spark ở provider 4.8.2.
