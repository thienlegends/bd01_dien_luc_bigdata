"""
Xuất bảng gold/silver (Delta) ra Parquet nhỏ để web đọc bằng DuckDB (web KHÔNG đọc Delta trực tiếp).
Đây chính là bước `cap_nhat_cache_web` ở cuối DAG 1.

Đầu ra (HDFS): <goc>/cache_web/{phu_tai_15p, du_bao_phu_tai, canh_bao_bat_thuong, tieu_thu, du_lieu_tre}
Sau đó chạy scripts/dong_bo_cache.sh trên máy chạy web để kéo về thư mục cache/.

  spark-submit --master spark://master:7077 \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    cap_nhat_cache_web.py
"""
import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--goc", default="hdfs://master:9000/lakehouse/bd01_dien_luc")
    ap.add_argument("--so-ngay", type=int, default=60, help="số ngày gold/dự báo xuất ra")
    ap.add_argument("--so-ngay-tieu-thu", type=int, default=28, help="số ngày tiêu thụ cấp khách hàng")
    a = ap.parse_args()

    spark = SparkSession.builder.appName("cap_nhat_cache_web").getOrCreate()
    doc = lambda p: spark.read.format("delta").load(f"{a.goc}/{p}")
    ghi = lambda df, ten: (df.coalesce(1).write.mode("overwrite").parquet(f"{a.goc}/cache_web/{ten}"),
                           print("Đã xuất", ten))

    # --- gold.phu_tai_15p
    pt = doc("gold/phu_tai_15p")
    mx = pt.agg(F.max("ngay")).first()[0]
    if mx is None:
        print("gold.phu_tai_15p rỗng, dừng.")
        return
    ghi(pt.filter(F.col("ngay") >= F.date_sub(F.lit(mx), a.so_ngay))
          .select("thoi_diem", "tong_cong_suat_kw", "so_khach_hang_co_du_lieu", "ty_le_noi_suy"),
        "phu_tai_15p")

    # --- gold.du_bao_phu_tai (cần cột silver_version để màn hình time travel hoạt động)
    db = doc("gold/du_bao_phu_tai")
    if "silver_version" not in db.columns:
        db = db.withColumn("silver_version", F.lit(None).cast("long"))
    ghi(db.filter(F.col("thoi_diem_muc_tieu") >= F.date_sub(F.lit(mx), a.so_ngay).cast("timestamp"))
          .select("thoi_diem_du_bao", "thoi_diem_muc_tieu", "buoc", "gia_tri_du_bao",
                  "can_duoi", "can_tren", "phien_ban_mo_hinh", "silver_version"),
        "du_bao_phu_tai")

    # --- gold.canh_bao_bat_thuong
    ghi(doc("gold/canh_bao_bat_thuong")
        .select("ma_canh_bao", "ma_khach_hang", "ngay_phat_hien", "loai", "diem_bat_thuong",
                "bang_chung", "trang_thai"),
        "canh_bao_bat_thuong")

    # --- silver.doc_so: tiêu thụ cấp khách hàng (gần đây) + thống kê dữ liệu đến trễ theo ngày
    sv = doc("silver/doc_so")
    mx_sv = sv.agg(F.max("ngay")).first()[0]
    if mx_sv is not None:
        ghi(sv.filter(F.col("ngay") >= F.date_sub(F.lit(mx_sv), a.so_ngay_tieu_thu))
              .select("ma_khach_hang", "thoi_diem_doc", "cong_suat_kw", "chat_luong"),
            "tieu_thu")
        ghi(sv.filter(F.col("ngay") >= F.date_sub(F.lit(mx_sv), a.so_ngay))
              .groupBy("ngay")
              .agg(F.count("*").alias("tong_ban_ghi"),
                   F.sum(F.when(F.col("do_tre_phut") >= 60, 1).otherwise(0)).alias("so_ban_ghi_tre"),
                   F.sum(F.col("co_hieu_chinh").cast("int")).alias("so_hieu_chinh")),
            "du_lieu_tre")
    spark.stop()


if __name__ == "__main__":
    main()
