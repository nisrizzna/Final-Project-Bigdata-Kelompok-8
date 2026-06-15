#!/usr/bin/env python3
"""
KAFKA CONSUMER → HDFS + Local
==============================
Baca dari topic pangan-api dan pangan-rss.
Flush ke HDFS via WebHDFS REST API setiap 2 menit.
Simpan salinan lokal di dashboard/data/ untuk Flask.

Jalankan:
  python kafka/consumer_to_hdfs.py
"""

import json, time, logging, threading, os
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import requests
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
HDFS_URL        = os.getenv("HDFS_URL", "http://localhost:19870/webhdfs/v1")
HDFS_USER       = "root"
TOPICS          = ["pangan-api", "pangan-rss"]
FLUSH_INTERVAL  = 15  # 15 detik untuk mode demo/pengumpulan data cepat

LOCAL_DIR = Path(__file__).parent.parent / "dashboard" / "data"
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

_buffers: dict = defaultdict(list)
_lock          = threading.Lock()


# ════════════════════════════════════════════════════════
# WebHDFS Helpers
# ════════════════════════════════════════════════════════
def hdfs_mkdir(path: str) -> bool:
    url = f"{HDFS_URL}{path}?op=MKDIRS&user.name={HDFS_USER}"
    try:
        r = requests.put(url, timeout=10)
        return r.status_code in (200, 201)
    except Exception:
        return False


def hdfs_write(hdfs_path: str, content: str) -> bool:
    try:
        # Step 1: buat file di NameNode
        url1 = f"{HDFS_URL}{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
        r1   = requests.put(url1, allow_redirects=False, timeout=10)
        if r1.status_code != 307:
            return False
        # Step 2: upload ke DataNode
        r2 = requests.put(
            r1.headers["Location"],
            data=content.encode("utf-8"),
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        return r2.status_code == 201
    except requests.exceptions.ConnectionError:
        return False
    except Exception as e:
        log.debug(f"[HDFS] Error: {e}")
        return False


# ════════════════════════════════════════════════════════
# Flush ke HDFS + simpan lokal
# ════════════════════════════════════════════════════════
def flush_to_hdfs(topic: str, records: list):
    if not records:
        return

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_str = datetime.now().strftime("%Y/%m/%d")
    hdfs_dir = f"/data/pangan/{topic}/{date_str}"
    hdfs_path= f"{hdfs_dir}/batch_{ts}.json"
    content  = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    hdfs_mkdir(hdfs_dir)
    if hdfs_write(hdfs_path, content):
        log.info(f"  [HDFS] ✓ {len(records)} records → {hdfs_path}")
    else:
        log.warning(f"  [HDFS] Upload gagal — data tetap di file lokal")

    # Simpan lokal untuk Flask (rolling buffer)
    short = topic.replace("pangan-", "")
    local = LOCAL_DIR / f"live_{short}.json"
    
    existing = []
    if local.exists():
        try:
            with open(local, encoding="utf-8") as f:
                existing = json.load(f)
                if not isinstance(existing, list):
                    existing = []
        except Exception:
            existing = []

    if topic == "pangan-rss":
        # Gunakan set id untuk menghindari duplikasi artikel
        seen_ids = {r.get("id") for r in records if r.get("id")}
        merged = records + [r for r in existing if r.get("id") not in seen_ids]
        # Urutkan dari yang terbaru dan batasi 30 artikel
        merged = sorted(merged, key=lambda x: x.get("timestamp", ""), reverse=True)[:30]
    else:  # pangan-api
        merged = records + existing
        merged = sorted(merged, key=lambda x: x.get("timestamp", ""), reverse=True)[:200]

    with open(local, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    if topic == "pangan-api":
        # summary menggunakan records terbaru dari buffer untuk dashboard real-time
        _update_summary(merged)


def _update_summary(records: list):
    """Ringkasan harga terkini per komoditas untuk panel live dashboard."""
    summary = {}
    for r in records:
        k = r.get("komoditas")
        if k:
            summary[k] = {
                "harga_rp"        : r.get("harga_rp"),
                "satuan"          : r.get("satuan"),
                "kurs_usd_idr"    : r.get("kurs_usd_idr"),
                "kurs_change_pct" : r.get("kurs_change_pct"),
                "dampak_kurs_rp"  : r.get("dampak_kurs_rp"),
                "timestamp"       : r.get("timestamp"),
                "sumber"          : r.get("sumber_harga"),
            }
    out = LOCAL_DIR / "live_summary.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"updated_at": datetime.now().isoformat(), "data": summary},
                  f, ensure_ascii=False, indent=2)
    log.info(f"  [LOCAL] ✓ live_summary.json ({len(summary)} komoditas)")


# ════════════════════════════════════════════════════════
# Consumer Thread
# ════════════════════════════════════════════════════════
def consume_thread():
    while True:
        try:
            log.info(f"Menghubungkan ke Kafka di {KAFKA_BOOTSTRAP}...")
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="pangan-consumer-hdfs",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=5000,
            )
            log.info(f"✓ Consumer aktif — topic: {TOPICS}")
            while True:
                try:
                    for message in consumer:
                        with _lock:
                            _buffers[message.topic].append(message.value)
                except Exception as e:
                    log.warning(f"[CONSUMER] Loop error: {e}")
                    raise e
                time.sleep(1)
        except Exception as e:
            log.warning(f"[CONSUMER] Error: {e}. Mencoba menghubungkan kembali dalam 5 detik...")
            time.sleep(5)


# ════════════════════════════════════════════════════════
# Flush Thread
# ════════════════════════════════════════════════════════
def flush_thread():
    for path in ["/data", "/data/pangan", "/data/pangan/pangan-api", "/data/pangan/pangan-rss"]:
        hdfs_mkdir(path)
    log.info("✓ HDFS direktori diinisialisasi")

    while True:
        time.sleep(FLUSH_INTERVAL)
        with _lock:
            for topic, records in list(_buffers.items()):
                if records:
                    flush_to_hdfs(topic, records)
                    _buffers[topic] = []
            total = sum(len(v) for v in _buffers.values())
        log.info(f"  Buffer: {total} records menunggu flush berikutnya")


# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════
def main():
    log.info("=" * 55)
    log.info("  CONSUMER → HDFS + Local")
    log.info(f"  Kafka: {KAFKA_BOOTSTRAP} | Topics: {TOPICS}")
    log.info(f"  HDFS : {HDFS_URL} | Flush: setiap {FLUSH_INTERVAL}s")
    log.info("=" * 55)

    t_consumer = threading.Thread(target=consume_thread, daemon=True)
    t_flush    = threading.Thread(target=flush_thread, daemon=True)
    t_consumer.start()
    t_flush.start()

    log.info("✓ Consumer + Flush thread berjalan")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Dihentikan.")


if __name__ == "__main__":
    main()
