"""
Tạo dữ liệu THỬ (giả lập) vào thư mục cache/ để chạy và xem giao diện web ngay,
trước khi pipeline Spark có dữ liệu gold thật.

  python3 tao_du_lieu_demo.py            # ghi vào ./cache
  python3 tao_du_lieu_demo.py --so-khach-hang 100
Cần: pandas numpy pyarrow
"""
import argparse
import json
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

KET_THUC = datetime(2014, 12, 21, 12, 0)


def tao_ma_tran(rng, so_kh, so_ngay):
    """Ma trận tiêu thụ (thời gian x khách hàng) kéo dài thêm 1 ngày sau KET_THUC (để sinh dự báo)."""
    bat_dau = KET_THUC - timedelta(days=so_ngay) + timedelta(minutes=15)
    tg = pd.date_range(bat_dau, KET_THUC + timedelta(days=1), freq="15min")
    gio = tg.hour.values + tg.minute.values / 60.0
    hinh = (0.55 + 0.35 * np.exp(-((gio - 19) / 3.0) ** 2) + 0.2 * np.exp(-((gio - 8) / 2.5) ** 2))
    cuoi_tuan = np.where(tg.dayofweek.values >= 5, 0.85, 1.0)
    quy_mo = rng.lognormal(mean=1.0, sigma=0.5, size=so_kh)
    nhieu = rng.lognormal(0, 0.06, size=(len(tg), so_kh))
    return tg, (hinh * cuoi_tuan)[:, None] * quy_mo[None, :] * nhieu


def tao_du_lieu(so_kh=60, so_ngay=21, seed=7):
    rng = np.random.default_rng(seed)
    ma = [f"MT_{i + 1:03d}" for i in range(so_kh)]
    tg, m = tao_ma_tran(rng, so_kh, so_ngay)
    thuc = tg <= KET_THUC
    m_thuc = m.copy()

    # bất thường cấy: 3 khách sụt 40%, 3 khách tăng vọt về đêm (5 ngày cuối), 2 khách mất tín hiệu 8 giờ cuối
    chon = rng.choice(so_kh, size=8, replace=False)
    sut, tang, mat = chon[:3], chon[3:6], chon[6:8]
    cuoi = tg > KET_THUC - timedelta(days=5)
    dem = (tg.hour >= 22) | (tg.hour < 6)
    m_thuc[np.ix_(cuoi & thuc, sut)] *= 0.6
    m_thuc[np.ix_(cuoi & dem & thuc, tang)] *= 2.5
    mat_ts = (tg > KET_THUC - timedelta(hours=8)) & thuc
    m_thuc[np.ix_(mat_ts, mat)] = np.nan
    m_thuc[rng.random(m_thuc.shape) < 0.01] = np.nan  # thiếu ngẫu nhiên

    # tiêu thụ cấp khách hàng (dạng long)
    tg_thuc = tg[thuc]
    mt = m_thuc[thuc]
    tieu_thu = pd.DataFrame({
        "ma_khach_hang": np.repeat(ma, len(tg_thuc)),
        "thoi_diem_doc": np.tile(tg_thuc.values, so_kh),
        "cong_suat_kw": mt.T.reshape(-1),
    })
    tieu_thu["chat_luong"] = np.where(tieu_thu["cong_suat_kw"].isna(), "nghi_ngo", "tot")

    # gold.phu_tai_15p
    phu_tai = pd.DataFrame({
        "thoi_diem": tg_thuc,
        "tong_cong_suat_kw": np.nansum(mt, axis=1),
        "so_khach_hang_co_du_lieu": (~np.isnan(mt)).sum(axis=1).astype(int),
        "ty_le_noi_suy": rng.uniform(0, 0.02, size=len(tg_thuc)),
    })

    # gold.du_bao_phu_tai: chạy dự báo mỗi 6 giờ trong 3 ngày cuối; phiên bản silver tăng dần,
    # phiên bản càng mới (có thêm dữ liệu trễ) sai số càng nhỏ
    tong_that = np.nansum(m, axis=1)
    chi_so = {t: i for i, t in enumerate(tg)}
    cac_lan = pd.date_range(KET_THUC - timedelta(days=3), KET_THUC, freq="6h")
    dong = []
    for k, t_run in enumerate(cac_lan):
        sai_so = 0.05 - 0.035 * k / max(1, len(cac_lan) - 1)
        for buoc in range(1, 97):
            tm = t_run + timedelta(minutes=15 * buoc)
            if tm not in chi_so:
                continue
            f = tong_that[chi_so[tm]] * (1 + rng.normal(0, sai_so) + 0.01 * (len(cac_lan) - k) / len(cac_lan))
            r = 0.02 + 0.06 * buoc / 96
            dong.append((t_run, tm, buoc, f, f * (1 - 1.96 * r), f * (1 + 1.96 * r),
                         "gbt_v20141201", 100 + k))
    du_bao = pd.DataFrame(dong, columns=["thoi_diem_du_bao", "thoi_diem_muc_tieu", "buoc", "gia_tri_du_bao",
                                         "can_duoi", "can_tren", "phien_ban_mo_hinh", "silver_version"])

    # gold.canh_bao_bat_thuong
    ngay = KET_THUC.date()
    cb = []

    def them(i, loai, diem, bang_chung, tt="moi"):
        cb.append((str(uuid.uuid4()), ma[i], ngay, loai, diem, json.dumps(bang_chung, ensure_ascii=False), tt))

    for i in sut:
        them(i, "sut_giam", round(float(rng.uniform(0.75, 0.98)), 2),
             {"ty_le_giam": 0.4, "so_buoc_lien_tiep": int(rng.integers(40, 400)), "residual_sigma": round(float(rng.uniform(3.2, 6)), 1)})
    for i in tang:
        them(i, "tang_vot", round(float(rng.uniform(0.7, 0.95)), 2),
             {"khung_gio": "22h-06h", "ty_le_tang": 2.5, "so_buoc_lien_tiep": int(rng.integers(30, 200))})
    for i in mat:
        them(i, "mat_tin_hieu", round(float(rng.uniform(0.6, 0.9)), 2), {"gio_mat_du_lieu": 8, "gio_hanh_chinh": True})
    for i in rng.choice([x for x in range(so_kh) if x not in chon], size=3, replace=False):
        them(i, "lech_cum", round(float(rng.uniform(0.3, 0.6)), 2), {"khoang_cach_tam_cum": round(float(rng.uniform(2, 4)), 2), "cum": int(rng.integers(0, 6))})
    cb[0] = cb[0][:6] + ("xac_nhan",)
    canh_bao = pd.DataFrame(cb, columns=["ma_canh_bao", "ma_khach_hang", "ngay_phat_hien", "loai",
                                         "diem_bat_thuong", "bang_chung", "trang_thai"])

    # thống kê dữ liệu đến trễ theo ngày
    ngay_ds = pd.date_range(KET_THUC.date() - timedelta(days=so_ngay - 1), KET_THUC.date(), freq="D")
    tong = so_kh * 96
    tre = pd.DataFrame({
        "ngay": ngay_ds.date,
        "tong_ban_ghi": tong,
        "so_ban_ghi_tre": (tong * rng.uniform(0.04, 0.06, len(ngay_ds))).astype(int),
        "so_hieu_chinh": (tong * rng.uniform(0.008, 0.012, len(ngay_ds))).astype(int),
    })
    return {"phu_tai_15p": phu_tai, "du_bao_phu_tai": du_bao, "canh_bao_bat_thuong": canh_bao,
            "tieu_thu": tieu_thu, "du_lieu_tre": tre}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", default=str(Path(__file__).parent / "cache"))
    ap.add_argument("--so-khach-hang", type=int, default=60)
    ap.add_argument("--so-ngay", type=int, default=21)
    a = ap.parse_args()
    cache_root = Path(a.cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    (cache_root / "_MODE_DEMO.json").write_text(
        json.dumps({"mode": "demo", "dataset": "synthetic", "generated_at": datetime.now().isoformat(timespec="seconds")}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    for ten, df in tao_du_lieu(a.so_khach_hang, a.so_ngay).items():
        d = Path(a.cache_dir) / ten
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
        df.to_parquet(d / "part-0.parquet", index=False)
        print(f"{ten}: {len(df):,} dòng")
    print("Xong. Chạy: uvicorn app.main:app --host 0.0.0.0 --port 8000")


if __name__ == "__main__":
    main()
