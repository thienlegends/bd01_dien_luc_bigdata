"""Cấu hình web. Mọi giá trị đổi được bằng biến môi trường."""
import os
import shlex
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Thư mục cache Parquet (mỗi bảng một thư mục con: phu_tai_15p/, du_bao_phu_tai/, ...)
CACHE_DIR = Path(os.getenv("CACHE_DIR", BASE_DIR / "cache"))
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
SQLITE_PATH = DATA_DIR / "xac_nhan.sqlite"

# Airflow REST API (Airflow 2.x, bật basic_auth). Spark Master UI đã dùng cổng 8080
# nên nên chạy Airflow webserver ở cổng 8081.
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://master:8081").rstrip("/")
AIRFLOW_API = os.getenv("AIRFLOW_API", "/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")

# Ghi ngược vào Delta: web gọi spark-submit chạy spark_jobs/ghi_xac_nhan_delta.py
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://master:7077")
DELTA_PKG = os.getenv("DELTA_PKG", "io.delta:delta-spark_2.12:3.2.0")
LAKEHOUSE = os.getenv("LAKEHOUSE", "hdfs://master:9000/lakehouse/bd01_dien_luc")
GHI_DELTA_JOB = BASE_DIR / "spark_jobs" / "ghi_xac_nhan_delta.py"
DONG_BO_CACHE_SH = BASE_DIR / "scripts" / "dong_bo_cache.sh"


def lenh_ghi_delta(payload_json):
    """Trả về lệnh (list) ghi xác nhận vào Delta, hoặc None nếu tắt (GHI_DELTA_CMD=off).

    Có thể tự đặt lệnh qua GHI_DELTA_CMD, dùng "{json}" làm chỗ chèn dữ liệu."""
    mau = os.getenv("GHI_DELTA_CMD")
    if mau and mau.strip().lower() == "off":
        return None
    if mau:
        return [payload_json if x == "{json}" else x for x in shlex.split(mau)]
    return [
        "spark-submit", "--master", SPARK_MASTER,
        "--packages", DELTA_PKG,
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        str(GHI_DELTA_JOB), "--goc", LAKEHOUSE, "--json", payload_json,
    ]
