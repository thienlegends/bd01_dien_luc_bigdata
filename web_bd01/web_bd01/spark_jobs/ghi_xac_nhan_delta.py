"""
Ghi ngược kết luận xử lý cảnh báo từ web vào Delta:
  1. Ghi vào silver.xac_nhan_canh_bao (không ghi trùng cùng ma_canh_bao + thoi_diem)
  2. MERGE cập nhật trang_thai trong gold.canh_bao_bat_thuong

Web gọi file này bằng spark-submit (xem app/config.py), truyền dữ liệu qua --json.
"""
import argparse
import json

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window, functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--goc", default="hdfs://master:9000/lakehouse/bd01_dien_luc")
    ap.add_argument("--json", required=True)
    a = ap.parse_args()

    spark = SparkSession.builder.appName("ghi_xac_nhan_delta").getOrCreate()
    rows = json.loads(a.json)
    df = (spark.createDataFrame(
            [(r["ma_canh_bao"], r["nguoi_xu_ly"], r["thoi_diem"], r["ket_luan"], r.get("ghi_chu", "")) for r in rows],
            "ma_canh_bao string, nguoi_xu_ly string, thoi_diem string, ket_luan string, ghi_chu string")
          .withColumn("thoi_diem", F.to_timestamp("thoi_diem")))

    # 1. silver.xac_nhan_canh_bao
    xn = f"{a.goc}/silver/xac_nhan_canh_bao"
    if DeltaTable.isDeltaTable(spark, xn):
        (DeltaTable.forPath(spark, xn).alias("t")
         .merge(df.alias("s"), "t.ma_canh_bao = s.ma_canh_bao AND t.thoi_diem = s.thoi_diem")
         .whenNotMatchedInsertAll().execute())
    else:
        df.write.format("delta").mode("append").save(xn)

    # 2. MERGE gold.canh_bao_bat_thuong (lấy kết luận mới nhất của mỗi cảnh báo)
    w = Window.partitionBy("ma_canh_bao").orderBy(F.col("thoi_diem").desc())
    moi = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    (DeltaTable.forPath(spark, f"{a.goc}/gold/canh_bao_bat_thuong").alias("t")
     .merge(moi.alias("s"), "t.ma_canh_bao = s.ma_canh_bao")
     .whenMatchedUpdate(set={"trang_thai": "s.ket_luan"})
     .execute())
    print("Đã ghi", df.count(), "xác nhận vào Delta")
    spark.stop()


if __name__ == "__main__":
    main()
