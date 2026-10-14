"""Truy vấn cache Parquet bằng DuckDB + lưu xác nhận cảnh báo (tạm) bằng SQLite.

Web KHÔNG đọc trực tiếp bảng Delta lớn: dữ liệu gold/silver đã được job
spark_jobs/cap_nhat_cache_web.py xuất ra Parquet, web chỉ đọc các file đó.
"""
import json
import math
import sqlite3
from datetime import datetime

import duckdb

from . import config

FMT = "%Y-%m-%d %H:%M:%S"


class ThieuCache(Exception):
    """Chưa có file cache cho một bảng."""


# --------------------------------------------------------------------------- cache Parquet
def _thu_muc(ten):
    return config.CACHE_DIR / ten


def co_bang(ten):
    d = _thu_muc(ten)
    return d.is_dir() and any(d.glob("*.parquet"))


def _bang(ten):
    if not co_bang(ten):
        raise ThieuCache(
            f"Chưa có cache cho bảng '{ten}' trong {config.CACHE_DIR}. Chạy tao_du_lieu_demo.py "
            "(dữ liệu thử) hoặc cap_nhat_cache_web.py + scripts/dong_bo_cache.sh (dữ liệu thật).")
    p = (_thu_muc(ten) / "*.parquet").as_posix().replace("'", "''")
    return f"read_parquet('{p}')"


def cache_mtime():
    d = _thu_muc("phu_tai_15p")
    if not d.is_dir():
        return 0.0
    ts = [f.stat().st_mtime for f in d.glob("*.parquet")]
    return max(ts) if ts else 0.0


def cache_cap_nhat_str():
    m = cache_mtime()
    return datetime.fromtimestamp(m).strftime("%d/%m/%Y %H:%M") if m else "chưa có"


def _sach(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def truy_van(sql, tham_so=None):
    con = duckdb.connect(":memory:")
    try:
        cur = con.execute(sql, tham_so or [])
        cot = [d[0] for d in cur.description]
        return [{k: _sach(v) for k, v in zip(cot, r)} for r in cur.fetchall()]
    finally:
        con.close()


def _json(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return {"raw": v}
    return v


# --------------------------------------------------------------------------- tổng quan
def moc_du_lieu():
    r = truy_van(f"SELECT max(thoi_diem) AS m FROM {_bang('phu_tai_15p')}")
    return r[0]["m"]


def phu_tai(tu, den):
    return truy_van(
        f"""SELECT thoi_diem, tong_cong_suat_kw, so_khach_hang_co_du_lieu, ty_le_noi_suy
            FROM {_bang('phu_tai_15p')}
            WHERE thoi_diem >= ? AND thoi_diem <= ? ORDER BY thoi_diem""", [tu, den])


def du_bao_moi_nhat():
    b = _bang("du_bao_phu_tai")
    rows = truy_van(f"""
        WITH m AS (SELECT max(thoi_diem_du_bao) AS t FROM {b})
        SELECT d.thoi_diem_du_bao, d.thoi_diem_muc_tieu, d.buoc, d.gia_tri_du_bao,
               d.can_duoi, d.can_tren, d.phien_ban_mo_hinh, d.silver_version
        FROM {b} d JOIN m ON d.thoi_diem_du_bao = m.t ORDER BY d.buoc""")
    if not rows:
        return {"thoi_diem_du_bao": None, "phien_ban_mo_hinh": None, "silver_version": None, "diem": []}
    return {
        "thoi_diem_du_bao": rows[0]["thoi_diem_du_bao"],
        "phien_ban_mo_hinh": rows[0]["phien_ban_mo_hinh"],
        "silver_version": rows[0]["silver_version"],
        "diem": [{k: r[k] for k in ("thoi_diem_muc_tieu", "buoc", "gia_tri_du_bao", "can_duoi", "can_tren")}
                 for r in rows],
    }


def kpi():
    pt, dbao = _bang("phu_tai_15p"), _bang("du_bao_phu_tai")
    # MAPE 7 ngày gần nhất (tính theo mốc dữ liệu mới nhất), trên mọi bước dự báo đã có giá trị thực
    m = truy_van(f"""
        WITH moc AS (SELECT max(thoi_diem) AS m FROM {pt}),
        d AS (
            SELECT b.buoc, abs(b.gia_tri_du_bao - p.tong_cong_suat_kw) / p.tong_cong_suat_kw AS e
            FROM {dbao} b JOIN {pt} p ON p.thoi_diem = b.thoi_diem_muc_tieu CROSS JOIN moc
            WHERE b.thoi_diem_muc_tieu > moc.m - INTERVAL 7 DAY AND p.tong_cong_suat_kw > 0
        )
        SELECT avg(e) * 100 AS mape,
               avg(e) FILTER (WHERE buoc <= 4) * 100 AS mape_1_4,
               avg(e) FILTER (WHERE buoc BETWEEN 5 AND 24) * 100 AS mape_5_24,
               avg(e) FILTER (WHERE buoc >= 25) * 100 AS mape_25_96,
               count(*) AS so_mau
        FROM d""")[0]

    cb = canh_bao(gioi_han=100000) if co_bang("canh_bao_bat_thuong") else []
    ngay_moi_nhat = max((c["ngay_phat_hien"] for c in cb), default=None)
    mat = {c["ma_khach_hang"] for c in cb
           if c["loai"] == "mat_tin_hieu" and c["trang_thai"] != "bo_qua"
           and c["ngay_phat_hien"] == ngay_moi_nhat}
    return {
        "mape_7ngay": m["mape"], "mape_1_4": m["mape_1_4"], "mape_5_24": m["mape_5_24"],
        "mape_25_96": m["mape_25_96"], "so_mau": m["so_mau"],
        "so_khach_hang_mat_tin_hieu": len(mat),
        "so_canh_bao_moi": sum(1 for c in cb if c["trang_thai"] == "moi"),
        "moc_du_lieu": moc_du_lieu(),
    }


# --------------------------------------------------------------------------- cảnh báo
def canh_bao(loai=None, trang_thai=None, diem_min=0.0, gioi_han=500):
    dk, tham = ["coalesce(diem_bat_thuong, 0) >= ?"], [diem_min]
    if loai:
        dk.append("loai = ?")
        tham.append(loai)
    rows = truy_van(
        f"""SELECT ma_canh_bao, ma_khach_hang, ngay_phat_hien, loai, diem_bat_thuong,
                   bang_chung, trang_thai
            FROM {_bang('canh_bao_bat_thuong')}
            WHERE {' AND '.join(dk)}
            ORDER BY diem_bat_thuong DESC, ngay_phat_hien DESC""", tham)
    ghi_de = ghi_de_trang_thai()
    out = []
    for r in rows:
        if r["ma_canh_bao"] in ghi_de:
            r["trang_thai"] = ghi_de[r["ma_canh_bao"]]
        if trang_thai and r["trang_thai"] != trang_thai:
            continue
        r["bang_chung"] = _json(r["bang_chung"])
        out.append(r)
        if len(out) >= gioi_han:
            break
    return out


def canh_bao_chi_tiet(ma):
    rows = truy_van(
        f"""SELECT ma_canh_bao, ma_khach_hang, ngay_phat_hien, loai, diem_bat_thuong, bang_chung, trang_thai
            FROM {_bang('canh_bao_bat_thuong')} WHERE ma_canh_bao = ?""", [ma])
    if not rows:
        return None
    r = rows[0]
    r["bang_chung"] = _json(r["bang_chung"])
    ghi_de = ghi_de_trang_thai()
    if ma in ghi_de:
        r["trang_thai"] = ghi_de[ma]
    return r


def tieu_thu(ma_khach_hang, ngay=10):
    """Chuỗi tiêu thụ `ngay` ngày gần nhất + đường bao 'bình thường'
    (phân vị 5%-95% theo từng khung 15 phút trong toàn bộ lịch sử đã cache)."""
    tt = _bang("tieu_thu")
    return truy_van(f"""
        WITH kh AS (
            SELECT thoi_diem_doc, cong_suat_kw,
                   hour(thoi_diem_doc) * 60 + minute(thoi_diem_doc) AS khung
            FROM {tt} WHERE ma_khach_hang = ?),
        moc AS (SELECT max(thoi_diem_doc) AS m FROM kh),
        bao AS (
            SELECT khung, quantile_cont(cong_suat_kw, 0.05) AS lo, quantile_cont(cong_suat_kw, 0.95) AS hi
            FROM kh WHERE cong_suat_kw IS NOT NULL GROUP BY khung)
        SELECT k.thoi_diem_doc, k.cong_suat_kw, b.lo AS bao_duoi, b.hi AS bao_tren
        FROM kh k CROSS JOIN moc LEFT JOIN bao b ON b.khung = k.khung
        WHERE k.thoi_diem_doc > moc.m - to_days(CAST(? AS INTEGER))
        ORDER BY k.thoi_diem_doc""", [ma_khach_hang, ngay])


# --------------------------------------------------------------------------- time travel
def ngay_co_nhieu_phien_ban():
    return truy_van(f"""
        SELECT CAST(thoi_diem_muc_tieu AS DATE) AS ngay,
               count(DISTINCT silver_version) AS so_phien_ban
        FROM {_bang('du_bao_phu_tai')} GROUP BY 1 ORDER BY 1 DESC LIMIT 60""")


def timetravel(ngay, phien_ban=None):
    """Dự báo cho `ngay` theo từng phiên bản silver (mỗi phiên bản lấy lần chạy dự báo mới nhất)."""
    b, pt = _bang("du_bao_phu_tai"), _bang("phu_tai_15p")
    rows = truy_van(f"""
        WITH d AS (SELECT * FROM {b} WHERE CAST(thoi_diem_muc_tieu AS DATE) = ?),
        v AS (SELECT silver_version, max(thoi_diem_du_bao) AS t FROM d GROUP BY silver_version)
        SELECT d.silver_version AS phien_ban, d.thoi_diem_du_bao, d.thoi_diem_muc_tieu,
               d.gia_tri_du_bao, p.tong_cong_suat_kw AS thuc_te
        FROM d JOIN v ON d.silver_version IS NOT DISTINCT FROM v.silver_version
                     AND d.thoi_diem_du_bao = v.t
        LEFT JOIN {pt} p ON p.thoi_diem = d.thoi_diem_muc_tieu
        ORDER BY d.thoi_diem_du_bao, d.thoi_diem_muc_tieu""", [ngay])
    thuc_te = truy_van(
        f"""SELECT thoi_diem, tong_cong_suat_kw AS gia_tri FROM {pt}
            WHERE CAST(thoi_diem AS DATE) = ? ORDER BY thoi_diem""", [ngay])

    loc = {x.strip() for x in phien_ban.split(",")} if phien_ban else None
    theo = {}
    for r in rows:
        key = "khong_ro" if r["phien_ban"] is None else str(r["phien_ban"])
        if loc and key not in loc:
            continue
        pb = theo.setdefault(key, {"phien_ban": key, "thoi_diem_du_bao": r["thoi_diem_du_bao"],
                                   "diem": [], "_loi": []})
        pb["diem"].append({"thoi_diem": r["thoi_diem_muc_tieu"], "gia_tri": r["gia_tri_du_bao"]})
        if r["thuc_te"] and r["thuc_te"] > 0:
            pb["_loi"].append(abs(r["gia_tri_du_bao"] - r["thuc_te"]) / r["thuc_te"])
    ds = list(theo.values())
    for pb in ds:
        loi = pb.pop("_loi")
        pb["so_diem"] = len(pb["diem"])
        pb["mape"] = sum(loi) / len(loi) * 100 if loi else None

    so_sanh = []
    for a, c in zip(ds, ds[1:]):
        gt_a = {p["thoi_diem"]: p["gia_tri"] for p in a["diem"]}
        chung = [(gt_a[p["thoi_diem"]], p["gia_tri"]) for p in c["diem"] if p["thoi_diem"] in gt_a]
        if chung:
            kw = sum(abs(x - y) for x, y in chung) / len(chung)
            pct = sum(abs(x - y) / x for x, y in chung if x) / len(chung) * 100
        else:
            kw = pct = None
        so_sanh.append({"tu_phien_ban": a["phien_ban"], "den_phien_ban": c["phien_ban"],
                        "so_diem_chung": len(chung), "sai_lech_tb_kw": kw, "sai_lech_tb_pct": pct})

    tre = None
    if co_bang("du_lieu_tre"):
        t = truy_van(f"SELECT * FROM {_bang('du_lieu_tre')} WHERE ngay = ?", [ngay])
        tre = t[0] if t else None
    return {"ngay": str(ngay), "thuc_te": thuc_te, "phien_ban": ds, "so_sanh": so_sanh, "du_lieu_tre": tre}


# --------------------------------------------------------------------------- SQLite: xác nhận cảnh báo
def _kn():
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(config.SQLITE_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE IF NOT EXISTS xac_nhan (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ma_canh_bao TEXT NOT NULL, nguoi_xu_ly TEXT NOT NULL, thoi_diem TEXT NOT NULL,
        ket_luan TEXT NOT NULL, ghi_chu TEXT,
        da_dong_bo INTEGER NOT NULL DEFAULT 0,   -- 0 chờ, 1 đã ghi Delta, 2 tắt đồng bộ, -1 lỗi
        thong_bao TEXT)""")
    return c


def ghi_xac_nhan(ma, nguoi, ket_luan, ghi_chu):
    c = _kn()
    try:
        t = datetime.now().strftime(FMT)
        cur = c.execute("INSERT INTO xac_nhan (ma_canh_bao, nguoi_xu_ly, thoi_diem, ket_luan, ghi_chu) "
                        "VALUES (?,?,?,?,?)", (ma, nguoi, t, ket_luan, ghi_chu))
        c.commit()
        return cur.lastrowid
    finally:
        c.close()


def lay_xac_nhan(id_ghi):
    c = _kn()
    try:
        r = c.execute("SELECT * FROM xac_nhan WHERE id = ?", (id_ghi,)).fetchone()
        return dict(r) if r else None
    finally:
        c.close()


def danh_dau_dong_bo(id_ghi, trang_thai, thong_bao=""):
    c = _kn()
    try:
        c.execute("UPDATE xac_nhan SET da_dong_bo = ?, thong_bao = ? WHERE id = ?",
                  (trang_thai, thong_bao, id_ghi))
        c.commit()
    finally:
        c.close()


def ghi_de_trang_thai():
    """Trạng thái mới do web ghi mà cache chưa phản ánh (chưa đồng bộ, hoặc mới hơn cache)."""
    mtime = cache_mtime()
    c = _kn()
    try:
        rows = c.execute("SELECT ma_canh_bao, ket_luan, thoi_diem, da_dong_bo FROM xac_nhan ORDER BY id").fetchall()
    finally:
        c.close()
    res = {}
    for r in rows:
        t = datetime.strptime(r["thoi_diem"], FMT).timestamp()
        if r["da_dong_bo"] != 1 or t > mtime:
            res[r["ma_canh_bao"]] = r["ket_luan"]
    return res


def tinh_trang_dong_bo(ma):
    c = _kn()
    try:
        r = c.execute("SELECT * FROM xac_nhan WHERE ma_canh_bao = ? ORDER BY id DESC LIMIT 1", (ma,)).fetchone()
    finally:
        c.close()
    if not r:
        return {"co_ban_ghi": False}
    return {"co_ban_ghi": True, "da_dong_bo": r["da_dong_bo"], "thong_bao": r["thong_bao"],
            "ket_luan": r["ket_luan"], "thoi_diem": r["thoi_diem"], "nguoi_xu_ly": r["nguoi_xu_ly"]}
