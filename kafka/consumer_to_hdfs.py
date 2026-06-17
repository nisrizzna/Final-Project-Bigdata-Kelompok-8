#!/usr/bin/env python3
"""
KAFKA CONSUMER → HDFS + Local
==============================
Baca dari topic pangan-api dan pangan-rss.
Flush ke HDFS via WebHDFS REST API setiap 2 menit.
Simpan salinan lokal di dashboard/data/ untuk Flask.

Tambahan fitur:
  - Struktur HDFS time-series friendly per komoditas per tanggal:
      /data/pangan/timeseries/{komoditas}/{tanggal}.json
  - Agregasi harian: tiap flush, ringkasan harga per hari diperbarui
  - Output lokal baru: dashboard/data/timeseries_{komoditas}.json
      berisi array 30 hari, langsung dibaca Orang 3 untuk grafik historis
  - _update_summary diperkaya dengan field baru dari producer_api revisi
      (harga_kemarin, kurs_7d_avg, flag musiman)

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

# [BARU] State agregasi harian per komoditas
# Format: {"Beras IR I": {"2026-06-15": {"harga_avg": 17500, "count": 12, ...}}}
_agregasi_harian: dict = defaultdict(dict)

# WebHDFS Helpers (tidak diubah)
def hdfs_mkdir(path: str) -> bool:
    url = f"{HDFS_URL}{path}?op=MKDIRS&user.name={HDFS_USER}"
    try:
        r = requests.put(url, timeout=10)
        return r.status_code in (200, 201)
    except Exception:
        return False


def hdfs_write(hdfs_path: str, content: str) -> bool:
    try:
        url1 = f"{HDFS_URL}{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
        r1   = requests.put(url1, allow_redirects=False, timeout=10)
        if r1.status_code != 307:
            return False
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

# [BARU] Helpers HDFS Timeseries per Komoditas
def _nama_ke_slug(komoditas: str) -> str:
    """
    Ubah nama komoditas jadi slug aman untuk nama file/folder.
    Contoh: "Beras IR I" → "beras_ir_i"
    """
    return komoditas.lower().replace(" ", "_")


def hdfs_simpan_timeseries_komoditas(komoditas: str, tanggal: str, ringkasan: dict):
    """
    Simpan ringkasan harian satu komoditas ke HDFS dengan struktur:
      /data/pangan/timeseries/{slug_komoditas}/{tanggal}.json

    File ini berisi satu baris ringkasan hari itu:
    harga rata-rata, min, max, kurs avg, flag musiman.
    Ini yang dibaca Orang 2 (Spark) untuk training model time series.
    """
    slug      = _nama_ke_slug(komoditas)
    hdfs_dir  = f"/data/pangan/timeseries/{slug}"
    hdfs_path = f"{hdfs_dir}/{tanggal}.json"
    content   = json.dumps(ringkasan, ensure_ascii=False)

    hdfs_mkdir(hdfs_dir)
    if hdfs_write(hdfs_path, content):
        log.info(f"  [HDFS-TS] ✓ {komoditas} | {tanggal} → {hdfs_path}")
    else:
        log.debug(f"  [HDFS-TS] Gagal tulis {hdfs_path} (HDFS mungkin belum ready)")

# [BARU] Agregasi Harian per Komoditas
def _update_agregasi_harian(records: list):
    """
    Untuk setiap record dari topic pangan-api, akumulasi data harian:
    hitung running sum untuk harga, kurs, dan catat flag musiman.

    Dipanggil setiap flush agar agregasi selalu up-to-date.
    """
    for r in records:
        komoditas = r.get("komoditas")
        tanggal   = r.get("tanggal")
        harga     = r.get("harga_rp", 0)
        if not komoditas or not tanggal or harga <= 0:
            continue

        key = tanggal
        if key not in _agregasi_harian[komoditas]:
            _agregasi_harian[komoditas][key] = {
                "tanggal"          : tanggal,
                "komoditas"        : komoditas,
                "harga_sum"        : 0,
                "harga_min"        : harga,
                "harga_max"        : harga,
                "kurs_sum"         : 0,
                "count"            : 0,
                # Field musiman diambil dari record pertama hari itu
                "flag_lebaran"     : r.get("flag_lebaran", 0),
                "flag_panen"       : r.get("flag_panen", 0),
                "flag_impor_ekspor": r.get("flag_impor_ekspor", 0),
                "musim_event"      : r.get("musim_event", "Normal"),
            }

        bucket = _agregasi_harian[komoditas][key]
        bucket["harga_sum"] += harga
        bucket["kurs_sum"]  += r.get("kurs_usd_idr", 0)
        bucket["count"]     += 1
        bucket["harga_min"]  = min(bucket["harga_min"], harga)
        bucket["harga_max"]  = max(bucket["harga_max"], harga)


def _build_ringkasan_harian(komoditas: str, tanggal: str) -> dict | None:
    """
    Bangun dict ringkasan final dari akumulasi harian.
    Dipanggil saat flush untuk disimpan ke HDFS dan file lokal timeseries.
    """
    bucket = _agregasi_harian.get(komoditas, {}).get(tanggal)
    if not bucket or bucket["count"] == 0:
        return None

    count = bucket["count"]
    return {
        "tanggal"          : tanggal,
        "komoditas"        : komoditas,
        "harga_avg"        : round(bucket["harga_sum"] / count),
        "harga_min"        : bucket["harga_min"],
        "harga_max"        : bucket["harga_max"],
        "kurs_avg"         : round(bucket["kurs_sum"] / count),
        "flag_lebaran"     : bucket["flag_lebaran"],
        "flag_panen"       : bucket["flag_panen"],
        "flag_impor_ekspor": bucket["flag_impor_ekspor"],
        "musim_event"      : bucket["musim_event"],
        "jumlah_data"      : count,
    }

# [BARU] Output Lokal Timeseries per Komoditas (untuk Orang 3)
def _simpan_timeseries_lokal(komoditas: str):
    """
    Simpan array 30 hari ringkasan harian ke file lokal:
      dashboard/data/timeseries_{slug}.json

    Format: array of dict, diurutkan tanggal ascending (terlama → terbaru).
    Ini yang langsung dibaca Orang 3 untuk menggambar line chart historis.

    Juga memanfaatkan field historis_30h dari producer jika tersedia,
    sebagai fallback tambahan data.
    """
    slug      = _nama_ke_slug(komoditas)
    out_path  = LOCAL_DIR / f"timeseries_{slug}.json"

    # Kumpulkan semua ringkasan harian yang ada di _agregasi_harian
    semua_hari = []
    for tanggal, _ in sorted(_agregasi_harian.get(komoditas, {}).items()):
        ringkasan = _build_ringkasan_harian(komoditas, tanggal)
        if ringkasan:
            semua_hari.append(ringkasan)

    # Ambil 30 hari terakhir saja
    semua_hari = semua_hari[-30:]

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(semua_hari, f, ensure_ascii=False, indent=2)

    log.info(f"  [LOCAL-TS] ✓ timeseries_{slug}.json ({len(semua_hari)} hari)")

# Flush ke HDFS + simpan lokal (diupdate)

def flush_to_hdfs(topic: str, records: list):
    if not records:
        return

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_str = datetime.now().strftime("%Y/%m/%d")
    tanggal  = datetime.now().strftime("%Y-%m-%d")

    # Path HDFS lama (tetap dipertahankan)
    hdfs_dir  = f"/data/pangan/{topic}/{date_str}"
    hdfs_path = f"{hdfs_dir}/batch_{ts}.json"
    content   = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    hdfs_mkdir(hdfs_dir)
    if hdfs_write(hdfs_path, content):
        log.info(f"  [HDFS] ✓ {len(records)} records → {hdfs_path}")
    else:
        log.warning(f"  [HDFS] Upload gagal — data tetap di file lokal")

    # Simpan lokal untuk Flask (rolling buffer, tidak diubah)
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
        seen_ids = {r.get("id") for r in records if r.get("id")}
        merged   = records + [r for r in existing if r.get("id") not in seen_ids]
        merged   = sorted(merged, key=lambda x: x.get("timestamp", ""), reverse=True)[:30]
    else:  # pangan-api
        merged = records + existing
        merged = sorted(merged, key=lambda x: x.get("timestamp", ""), reverse=True)[:200]

    with open(local, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # Proses khusus untuk topic pangan-api 
    if topic == "pangan-api":
        _update_summary(merged)

        # [BARU] Update agregasi harian dari records baru
        _update_agregasi_harian(records)

        # [BARU] Untuk setiap komoditas yang ada di records:
        #  1. Build ringkasan hari ini
        #  2. Simpan ke HDFS timeseries
        #  3. Update file lokal timeseries untuk Orang 3
        komoditas_di_batch = set(r.get("komoditas") for r in records if r.get("komoditas"))
        for komoditas in komoditas_di_batch:
            ringkasan = _build_ringkasan_harian(komoditas, tanggal)
            if ringkasan:
                # Simpan ke HDFS: /data/pangan/timeseries/{slug}/{tanggal}.json
                hdfs_simpan_timeseries_komoditas(komoditas, tanggal, ringkasan)
                # Simpan array 30 hari ke lokal: dashboard/data/timeseries_{slug}.json
                _simpan_timeseries_lokal(komoditas)

# _update_summary (diupdate — tambah field baru dari producer revisi)

def _update_summary(records: list):
    """
    Ringkasan harga terkini per komoditas untuk panel live dashboard.
    [DIUPDATE] Tambah field: harga_kemarin, kurs_7d_avg, flag musiman.
    """
    summary = {}
    for r in records:
        k = r.get("komoditas")
        if k and k not in summary:  # ambil yang paling baru (records sudah sorted desc)
            summary[k] = {
                # Field lama 
                "harga_rp"         : r.get("harga_rp"),
                "satuan"           : r.get("satuan"),
                "kurs_usd_idr"     : r.get("kurs_usd_idr"),
                "kurs_change_pct"  : r.get("kurs_change_pct"),
                "dampak_kurs_rp"   : r.get("dampak_kurs_rp"),
                "timestamp"        : r.get("timestamp"),
                "sumber"           : r.get("sumber_harga"),
                # [BARU] Field dari producer_api revisi
                "harga_kemarin"    : r.get("harga_kemarin"),
                "kurs_7d_avg"      : r.get("kurs_7d_avg"),
                "kurs_30d_avg"     : r.get("kurs_30d_avg"),
                "flag_lebaran"     : r.get("flag_lebaran", 0),
                "flag_panen"       : r.get("flag_panen", 0),
                "flag_impor_ekspor": r.get("flag_impor_ekspor", 0),
                "musim_event"      : r.get("musim_event", "Normal"),
            }

    out = LOCAL_DIR / "live_summary.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(
            {"updated_at": datetime.now().isoformat(), "data": summary},
            f, ensure_ascii=False, indent=2
        )
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

# Flush Thread (diupdate — init direktori HDFS timeseries)

def flush_thread():
    # Init direktori HDFS lama 
    for path in ["/data", "/data/pangan", "/data/pangan/pangan-api", "/data/pangan/pangan-rss"]:
        hdfs_mkdir(path)

    # [BARU] Init direktori HDFS timeseries
    hdfs_mkdir("/data/pangan/timeseries")
    log.info("✓ HDFS direktori diinisialisasi (termasuk /timeseries)")

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
    log.info("  [REVISI] Timeseries per komoditas aktif")   # [BARU]
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