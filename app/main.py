"""FastAPI: API + trang HTML (Jinja2) cho hệ thống giám sát và dự báo phụ tải điện."""
import json
import subprocess
import threading
from datetime import date, datetime, timedelta
from typing import Literal, Optional

import requests
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from . import airflow_client, config, db

app = FastAPI(title="Giám sát & dự báo phụ tải điện")
app.mount("/static", StaticFiles(directory=str(config.BASE_DIR / "app" / "static")), name="static")
templates = Jinja2Templates(directory=str(config.BASE_DIR / "app" / "templates"))


@app.exception_handler(db.ThieuCache)
async def _thieu_cache(request: Request, exc: db.ThieuCache):
    return JSONResponse(status_code=503, content={"detail": str(exc)})


def _ctx(trang, **kw):
    return {"trang": trang, "cache_cap_nhat": db.cache_cap_nhat_str(), **kw}


def _dt(s, cuoi_ngay=False):
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        raise HTTPException(422, f"Sai định dạng thời gian: {s!r} (dùng YYYY-MM-DD hoặc YYYY-MM-DDTHH:MM)")
    if cuoi_ngay and len(s) == 10:
        d = d + timedelta(days=1) - timedelta(seconds=1)
    return d


# ------------------------------------------------------------------ trang HTML
@app.get("/")
def trang_tong_quan(request: Request):
    return templates.TemplateResponse(request, "tong_quan.html", _ctx("tong_quan"))


@app.get("/canh-bao")
def trang_canh_bao(request: Request):
    return templates.TemplateResponse(request, "canh_bao.html", _ctx("canh_bao"))


@app.get("/canh-bao/{ma_canh_bao}")
def trang_xu_ly(request: Request, ma_canh_bao: str):
    return templates.TemplateResponse(request, "xu_ly.html", _ctx("canh_bao", ma=ma_canh_bao))


@app.get("/timetravel")
def trang_timetravel(request: Request):
    return templates.TemplateResponse(request, "timetravel.html", _ctx("timetravel"))


@app.get("/quan-tri")
def trang_quan_tri(request: Request):
    return templates.TemplateResponse(request, "quan_tri.html", _ctx("quan_tri"))


# ------------------------------------------------------------------ API: tổng quan
@app.get("/api/phu-tai")
def api_phu_tai(tu: Optional[str] = None, den: Optional[str] = None):
    moc = db.moc_du_lieu()
    if moc is None:
        raise HTTPException(404, "Chưa có dữ liệu phụ tải.")
    d = _dt(den, True) if den else moc
    t = _dt(tu) if tu else d - timedelta(days=7)
    return {"tu": t.isoformat(), "den": d.isoformat(), "diem": db.phu_tai(t, d)}


@app.get("/api/du-bao")
def api_du_bao():
    return db.du_bao_moi_nhat()


@app.get("/api/kpi")
def api_kpi():
    return db.kpi()


# ------------------------------------------------------------------ API: cảnh báo
@app.get("/api/canh-bao")
def api_canh_bao(loai: Optional[str] = None, trang_thai: Optional[str] = None,
                 diem_min: float = 0.0, gioi_han: int = 500):
    return db.canh_bao(loai or None, trang_thai or None, diem_min, min(max(gioi_han, 1), 5000))


@app.get("/api/canh-bao/{ma_canh_bao}")
def api_canh_bao_chi_tiet(ma_canh_bao: str):
    r = db.canh_bao_chi_tiet(ma_canh_bao)
    if not r:
        raise HTTPException(404, "Không tìm thấy cảnh báo.")
    r["dong_bo"] = db.tinh_trang_dong_bo(ma_canh_bao)
    return r


@app.get("/api/khach-hang/{ma_khach_hang}/tieu-thu")
def api_tieu_thu(ma_khach_hang: str, ngay: int = 10):
    rows = db.tieu_thu(ma_khach_hang, min(max(ngay, 1), 60))
    if not rows:
        raise HTTPException(404, "Không có dữ liệu tiêu thụ của khách hàng này.")
    return {"ma_khach_hang": ma_khach_hang, "diem": rows}


class KetLuan(BaseModel):
    nguoi_xu_ly: str = Field(min_length=1, max_length=100)
    ket_luan: Literal["xac_nhan", "bo_qua", "dang_kiem_tra"]
    ghi_chu: str = Field(default="", max_length=1000)


_khoa_ghi_delta = threading.Lock()


def _ghi_delta(id_ghi: int):
    """Chạy nền: ghi silver.xac_nhan_canh_bao + MERGE gold.canh_bao_bat_thuong bằng spark-submit."""
    b = db.lay_xac_nhan(id_ghi)
    payload = json.dumps([{k: b[k] for k in ("ma_canh_bao", "nguoi_xu_ly", "thoi_diem", "ket_luan", "ghi_chu")}],
                         ensure_ascii=False)
    cmd = config.lenh_ghi_delta(payload)
    if cmd is None:
        db.danh_dau_dong_bo(id_ghi, 2, "Đã tắt đồng bộ Delta (GHI_DELTA_CMD=off)")
        return
    with _khoa_ghi_delta:  # tuần tự hóa để các MERGE không tranh chấp nhau
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
            ok = r.returncode == 0
            tb = "" if ok else (r.stderr or r.stdout)[-800:]
        except (OSError, subprocess.TimeoutExpired) as e:
            ok, tb = False, str(e)
    db.danh_dau_dong_bo(id_ghi, 1 if ok else -1, tb)


@app.post("/api/canh-bao/{ma_canh_bao}/ketluan")
def api_ket_luan(ma_canh_bao: str, body: KetLuan, nen: BackgroundTasks):
    if not db.canh_bao_chi_tiet(ma_canh_bao):
        raise HTTPException(404, "Không tìm thấy cảnh báo.")
    id_ghi = db.ghi_xac_nhan(ma_canh_bao, body.nguoi_xu_ly.strip(), body.ket_luan, body.ghi_chu.strip())
    nen.add_task(_ghi_delta, id_ghi)
    return {"ma_canh_bao": ma_canh_bao, "trang_thai": body.ket_luan,
            "ghi_chu": "Đã ghi nhận. Đang đồng bộ vào Delta Lake (có thể mất vài chục giây)."}


@app.get("/api/canh-bao/{ma_canh_bao}/dong-bo")
def api_dong_bo(ma_canh_bao: str):
    return db.tinh_trang_dong_bo(ma_canh_bao)


# ------------------------------------------------------------------ API: time travel
@app.get("/api/timetravel/ngay")
def api_timetravel_ngay():
    return db.ngay_co_nhieu_phien_ban()


@app.get("/api/timetravel")
def api_timetravel(ngay: str, phien_ban: Optional[str] = None):
    try:
        n = date.fromisoformat(ngay)
    except ValueError:
        raise HTTPException(422, "ngay phải có dạng YYYY-MM-DD")
    return db.timetravel(n, phien_ban)


# ------------------------------------------------------------------ API: quản trị (Airflow)
@app.get("/api/dag")
def api_dag():
    return airflow_client.DANH_SACH_DAG


def _kiem_tra_dag(dag_id):
    if dag_id not in airflow_client.HOP_LE:
        raise HTTPException(404, "DAG không nằm trong danh sách quản lý.")


@app.get("/api/dag/{dag_id}/runs")
def api_dag_runs(dag_id: str, gioi_han: int = 10):
    _kiem_tra_dag(dag_id)
    try:
        return airflow_client.lich_su_chay(dag_id, min(max(gioi_han, 1), 50))
    except requests.RequestException as e:
        raise HTTPException(502, f"Không gọi được Airflow ({config.AIRFLOW_URL}): {e}")


class KichHoat(BaseModel):
    conf: dict = Field(default_factory=dict)


@app.post("/api/dag/{dag_id}/trigger")
def api_dag_trigger(dag_id: str, body: KichHoat = KichHoat()):
    _kiem_tra_dag(dag_id)
    if dag_id == "dl_backfill" and not (body.conf.get("start_date") and body.conf.get("end_date")):
        raise HTTPException(422, "dl_backfill cần conf.start_date và conf.end_date (YYYY-MM-DD).")
    try:
        return airflow_client.kich_hoat(dag_id, body.conf)
    except requests.RequestException as e:
        raise HTTPException(502, f"Không gọi được Airflow ({config.AIRFLOW_URL}): {e}")


@app.post("/api/cache/dong-bo")
def api_dong_bo_cache():
    """Kéo cache mới từ HDFS về (chạy scripts/dong_bo_cache.sh)."""
    r = subprocess.run(["bash", str(config.DONG_BO_CACHE_SH)], capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        raise HTTPException(500, (r.stderr or r.stdout)[-800:])
    return {"ket_qua": "đã đồng bộ cache", "cap_nhat": db.cache_cap_nhat_str()}
