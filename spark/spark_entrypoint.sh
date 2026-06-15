#!/bin/bash
# ============================================================
# spark_entrypoint.sh — Jalankan Spark Master & Auto-Scheduler
# ============================================================

echo "=== Memulai Spark Master Node ==="

# Install dependencies yang dibutuhkan jika belum ada
if ! python3 -c "import numpy, pandas" &>/dev/null; then
  echo "=== Menginstal numpy & pandas... ==="
  pip install --no-cache-dir numpy pandas
fi

/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master &

# Tunggu Spark Master siap
sleep 10

# Environment variables untuk analysis.py
export OUTPUT_DIR=/data/output
export LOCAL_INPUT_DIR=/data/output

echo "=== Auto-Scheduler Spark Analysis Aktif (setiap 2 menit) ==="
while true; do
  if [ -f /spark-apps/analysis.py ]; then
    echo "[$(date '+%H:%M:%S')] Memulai analisis berkala..."
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 /spark-apps/analysis.py
    echo "[$(date '+%H:%M:%S')] Analisis selesai. Menunggu 2 menit..."
  else
    echo "[WARN] analysis.py tidak ditemukan di /spark-apps"
  fi
  sleep 120
done
