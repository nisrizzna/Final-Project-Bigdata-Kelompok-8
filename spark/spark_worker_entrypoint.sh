#!/bin/bash
# ============================================================
# spark_worker_entrypoint.sh — Jalankan Spark Worker & Auto-Install
# ============================================================

echo "=== Memulai Spark Worker Node ==="

# Install dependencies yang dibutuhkan jika belum ada
if ! python3 -c "import numpy, pandas" &>/dev/null; then
  echo "=== Menginstal numpy & pandas... ==="
  pip install --no-cache-dir numpy pandas
fi

/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
