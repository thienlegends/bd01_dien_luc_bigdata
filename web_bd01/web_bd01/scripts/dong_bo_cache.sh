#!/usr/bin/env bash
# Kéo cache Parquet từ HDFS về thư mục cache/ của web (thay từng bảng bằng thao tác đổi tên, web không bị gián đoạn).
set -euo pipefail
HDFS_DIR="${HDFS_CACHE_DIR:-/lakehouse/bd01_dien_luc/cache_web}"
LOCAL_DIR="${CACHE_DIR:-$(cd "$(dirname "$0")/.." && pwd)/cache}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
hdfs dfs -get "$HDFS_DIR" "$TMP/cache_web"

mkdir -p "$LOCAL_DIR"
for d in "$TMP"/cache_web/*/; do
  ten="$(basename "$d")"
  rm -rf "$LOCAL_DIR/$ten.cu"
  [ -d "$LOCAL_DIR/$ten" ] && mv "$LOCAL_DIR/$ten" "$LOCAL_DIR/$ten.cu"
  mv "$TMP/cache_web/$ten" "$LOCAL_DIR/$ten"
  rm -rf "$LOCAL_DIR/$ten.cu"
done
# Khi đã lấy cache thật từ HDFS thành công, bỏ dấu nhận diện dữ liệu demo.
rm -f "$LOCAL_DIR/_MODE_DEMO.json"
echo "Đã đồng bộ cache vào $LOCAL_DIR"
