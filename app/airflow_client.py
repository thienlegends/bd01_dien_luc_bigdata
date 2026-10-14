"""Gọi Airflow REST API (xem lịch sử chạy DAG, kích hoạt DAG)."""
import requests

from . import config

DANH_SACH_DAG = [
    {"dag_id": "dl_pipeline_hang_gio", "lich": "0 * * * *",
     "mo_ta": "Pipeline hằng giờ: kiểm tra → silver → gold → đặc trưng → dự báo → cập nhật cache"},
    {"dag_id": "dl_huan_luyen_lai", "lich": "0 2 * * 0",
     "mo_ta": "Huấn luyện lại mô hình (2h sáng Chủ nhật), promote nếu MAPE tốt hơn"},
    {"dag_id": "dl_bao_tri", "lich": "0 3 * * *",
     "mo_ta": "Bảo trì hằng ngày: OPTIMIZE → VACUUM → thống kê HDFS"},
    {"dag_id": "dl_backfill", "lich": "thủ công",
     "mo_ta": "Chạy lại pipeline cho khoảng ngày (start_date, end_date)"},
]
HOP_LE = {d["dag_id"] for d in DANH_SACH_DAG}


def _url(dag_id):
    return f"{config.AIRFLOW_URL}{config.AIRFLOW_API}/dags/{dag_id}/dagRuns"


def lich_su_chay(dag_id, gioi_han=10):
    r = requests.get(_url(dag_id), params={"limit": gioi_han, "order_by": "-start_date"},
                     auth=(config.AIRFLOW_USER, config.AIRFLOW_PASS), timeout=10)
    r.raise_for_status()
    return [{
        "dag_run_id": x.get("dag_run_id"),
        "state": x.get("state"),
        "run_type": x.get("run_type"),
        "start_date": x.get("start_date"),
        "end_date": x.get("end_date"),
    } for x in r.json().get("dag_runs", [])]


def kich_hoat(dag_id, conf=None):
    r = requests.post(_url(dag_id), json={"conf": conf or {}},
                      auth=(config.AIRFLOW_USER, config.AIRFLOW_PASS), timeout=10)
    r.raise_for_status()
    x = r.json()
    return {"dag_run_id": x.get("dag_run_id"), "state": x.get("state")}
