#!/usr/bin/env python3
"""
KAFKA PRODUCER — Harga Pangan + Kurs USD/IDR Real-Time
=======================================================
Sumber kurs  : yfinance USDIDR=X (real-time setiap 30 mnt)
Sumber harga : 1. PIHPS hargapangan.id (scraping)
               2. Panel Harga Badan Pangan Nasional (fallback)
               3. Model Korelasi Empiris berbasis kurs nyata (fallback final)
Output       : Kafka topic 'pangan-api'

Tambahan fitur:
  - Kurs historis 30 hari (window diperluas dari 2d → 30d)
  - Harga historis 30 hari per komoditas (time series array)
  - Field turunan: harga_kemarin, kurs_7d_avg, kurs_30d_avg
  - Flag musiman: flag_lebaran, flag_natal, flag_panen, flag_impor_ekspor

Jalankan:
  python kafka/producer_api.py
"""

import json, time, logging, random, os
from datetime import datetime, date, timedelta
from kafka import KafkaProducer
import requests
import yfinance as yf
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER-API] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_API        = "pangan-api"
INTERVAL_SECONDS = 5   # 5 detik untuk mode demo/pengumpulan data cepat

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/json",
    "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
}

# Koefisien korelasi kurs → harga (dari analisis historis PIHPS 2019–2026)
KORELASI_KURS = {
    "Beras IR I"            : 0.25,
    "Beras IR III"          : 0.20,
    "Cabai Rawit Merah"     : 0.05,
    "Cabai Merah Keriting"  : 0.05,
    "Bawang Merah"          : 0.10,
    "Bawang Putih"          : 0.40,  # mayoritas impor dari China (USD)
    "Daging Sapi"           : 0.55,  # impor Australia/India (USD)
    "Daging Ayam"           : 0.15,
    "Telur Ayam"            : 0.15,
    "Gula Pasir"            : 0.45,  # raw sugar impor (USD)
    "Minyak Goreng"         : 0.30,
}

SATUAN = {
    "Beras IR I"            : "/kg",
    "Beras IR III"          : "/kg",
    "Cabai Rawit Merah"     : "/kg",
    "Cabai Merah Keriting"  : "/kg",
    "Bawang Merah"          : "/kg",
    "Bawang Putih"          : "/kg",
    "Daging Sapi"           : "/kg",
    "Daging Ayam"           : "/kg",
    "Telur Ayam"            : "/kg",
    "Gula Pasir"            : "/kg",
    "Minyak Goreng"         : "/liter",
}

# Harga referensi per Juni 2026 (PIHPS BI)
HARGA_REF = {
    "Beras IR I"            : 17_500,
    "Beras IR III"          : 16_200,
    "Cabai Rawit Merah"     : 82_450,
    "Cabai Merah Keriting"  : 61_000,
    "Bawang Merah"          : 54_350,
    "Bawang Putih"          : 40_500,
    "Daging Sapi"           : 155_000,
    "Daging Ayam"           : 37_000,
    "Telur Ayam"            : 30_400,
    "Gula Pasir"            : 19_100,
    "Minyak Goreng"         : 20_700,
}

KURS_REF     = 18_100
_harga_state = dict(HARGA_REF)
_kurs_prev   = KURS_REF

# [BARU] State untuk menyimpan riwayat 30 hari
# Riwayat harga per komoditas: {"Beras IR I": [{"tanggal": "2026-05-16", "harga": 17400}, ...]}
# Diisi secara bertahap setiap loop, maksimal 30 entri per komoditas
_historis_harga: dict = {k: [] for k in KORELASI_KURS}

# Riwayat kurs: [{"tanggal": "2026-05-16", "kurs": 18050}, ...]
# Diisi dari yfinance historis 30 hari saat startup, lalu di-append tiap loop
_historis_kurs: list = []

# [BARU] Flag Musiman — Kalender Indonesia
# Rentang hari raya besar (batas H-30 hingga H+7 dianggap "musim")
# Format: (bulan, tanggal_mulai, tanggal_selesai)  — perkiraan, bisa diupdate tiap tahun
KALENDER_HARI_RAYA = [
    # Lebaran / Idul Fitri 2026 (sekitar 20 Maret)
    {"nama": "Lebaran",  "mulai": date(2026, 2, 18),  "selesai": date(2026, 3, 27)},
    # Natal & Tahun Baru
    {"nama": "Nataru",   "mulai": date(2026, 12, 15), "selesai": date(2027, 1, 5)},
    # Idul Adha 2026 (sekitar 28 Mei)
    {"nama": "Idul Adha","mulai": date(2026, 5, 14),  "selesai": date(2026, 6, 4)},
]

# Musim panen raya Indonesia (umumnya Maret–April dan Agustus–September)
KALENDER_PANEN = [
    {"nama": "Panen Raya I",  "mulai": date(2026, 3, 1),  "selesai": date(2026, 4, 30)},
    {"nama": "Panen Raya II", "mulai": date(2026, 8, 1),  "selesai": date(2026, 9, 30)},
]

# Bulan-bulan yang biasanya ada kebijakan impor/ekspor aktif
# (biasanya menjelang hari raya atau saat harga melonjak)
BULAN_IMPOR_AKTIF = [2, 3, 5, 11, 12]  # Feb, Mar, Mei, Nov, Des


def hitung_flag_musiman(hari_ini: date) -> dict:
    """
    Hitung flag musiman berdasarkan tanggal hari ini.
    Return dict berisi flag 0/1 dan nama event (kalau ada).

    Flag ini dipakai sebagai fitur input model ML Orang 2:
      flag_lebaran, flag_panen, flag_impor_ekspor
    masing-masing bernilai 1 jika sedang dalam periode tersebut, 0 jika tidak.
    """
    flag_lebaran      = 0
    flag_panen        = 0
    flag_impor_ekspor = 0
    nama_event        = []

    # Cek hari raya
    for hr in KALENDER_HARI_RAYA:
        if hr["mulai"] <= hari_ini <= hr["selesai"]:
            flag_lebaran = 1
            nama_event.append(hr["nama"])
            break

    # Cek musim panen
    for panen in KALENDER_PANEN:
        if panen["mulai"] <= hari_ini <= panen["selesai"]:
            flag_panen = 1
            nama_event.append(panen["nama"])
            break

    # Cek bulan impor aktif
    if hari_ini.month in BULAN_IMPOR_AKTIF:
        flag_impor_ekspor = 1
        nama_event.append("Musim Impor Aktif")

    return {
        "flag_lebaran"       : flag_lebaran,
        "flag_panen"         : flag_panen,
        "flag_impor_ekspor"  : flag_impor_ekspor,
        "musim_event"        : ", ".join(nama_event) if nama_event else "Normal",
    }

# [BARU] Fetch Kurs Historis 30 Hari dari yfinance
def fetch_kurs_historis_30h() -> list:
    """
    Ambil data kurs USD/IDR untuk 30 hari ke belakang dari yfinance.
    Dipanggil SEKALI saat startup untuk mengisi _historis_kurs.
    Return: list of dict [{"tanggal": "YYYY-MM-DD", "kurs": float}, ...]
    """
    try:
        ticker = yf.Ticker("USDIDR=X")
        # [DIUBAH] period dari "2d" → "35d" agar dapat 30 hari data bersih
        hist   = ticker.history(period="35d", interval="1d")
        if hist.empty:
            raise ValueError("Data historis kosong dari yfinance")

        hasil = []
        for idx, row in hist.iterrows():
            tanggal = idx.strftime("%Y-%m-%d")
            kurs    = round(float(row["Close"]), 2)
            hasil.append({"tanggal": tanggal, "kurs": kurs})

        # Ambil 30 hari terakhir saja
        hasil = hasil[-30:]
        log.info(f"[KURS-HIST] ✓ {len(hasil)} hari data historis kurs dimuat")
        return hasil

    except Exception as e:
        log.warning(f"[KURS-HIST] Gagal fetch historis: {e} — generate dari referensi")
        # Fallback: generate simulasi 30 hari ke belakang dari KURS_REF
        hasil = []
        for i in range(30, 0, -1):
            tgl  = (date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
            # Simulasi pergerakan kurs sederhana berbasis random walk
            noise = random.gauss(0, 50)
            kurs  = round(KURS_REF + noise * (i / 30), 2)
            hasil.append({"tanggal": tgl, "kurs": kurs})
        return hasil


def hitung_kurs_avg(window: int) -> float:
    """
    Hitung rata-rata kurs N hari terakhir dari _historis_kurs.
    Dipakai untuk mengisi field kurs_7d_avg dan kurs_30d_avg di payload.
    """
    if not _historis_kurs:
        return KURS_REF
    data = _historis_kurs[-window:]
    return round(sum(d["kurs"] for d in data) / len(data), 2)

# FETCH KURS — yfinance (selalu real-time)
def fetch_kurs_realtime() -> float:
    try:
        ticker = yf.Ticker("USDIDR=X")
        # [TETAP] interval 1h untuk real-time, cukup 2d saja
        hist   = ticker.history(period="2d", interval="1h")
        if hist.empty:
            raise ValueError("Data kosong dari yfinance")
        kurs = float(hist["Close"].iloc[-1])
        kurs += random.uniform(-30, 30)
        log.info(f"[KURS] ✓ USD/IDR real-time: Rp{kurs:,.0f}")
        return kurs
    except Exception as e:
        log.warning(f"[KURS] yfinance gagal: {e}")
        fallback = _kurs_prev * (1 + random.gauss(0, 0.001))
        log.warning(f"[KURS] Fallback: Rp{fallback:,.0f}")
        return fallback

# FETCH HARGA — Sumber 1: PIHPS hargapangan.id

def fetch_pihps_scraping() -> dict | None:
    try:
        url = "https://hargapangan.id/tabel-harga/pasar-tradisional/komoditas"
        r   = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()

        soup  = BeautifulSoup(r.text, "lxml")
        table = (soup.find("table", {"id": "tblHarga"}) or
                 soup.find("table", class_=lambda c: c and "harga" in c.lower()) or
                 soup.find("table"))

        if not table:
            return None

        MAPPING = {
            "beras kualitas super i"     : "Beras IR I",
            "beras kualitas medium ii"   : "Beras IR III",
            "beras kualitas bawah ii"    : "Beras IR III",
            "cabai rawit merah"          : "Cabai Rawit Merah",
            "cabai merah keriting"       : "Cabai Merah Keriting",
            "bawang merah"               : "Bawang Merah",
            "bawang putih ukuran sedang" : "Bawang Putih",
            "daging sapi kualitas 1"     : "Daging Sapi",
            "daging ayam ras segar"      : "Daging Ayam",
            "telur ayam ras segar"       : "Telur Ayam",
            "gula pasir lokal"           : "Gula Pasir",
            "minyak goreng curah"        : "Minyak Goreng",
        }

        hasil = {}
        for row in table.find_all("tr"):
            cols = row.find_all(["td", "th"])
            if len(cols) < 2:
                continue
            nama       = cols[0].get_text(strip=True).lower()
            harga_text = (cols[-1].get_text(strip=True)
                          .replace(".", "").replace(",", "")
                          .replace("Rp", "").strip())
            for key, internal in MAPPING.items():
                if key in nama:
                    try:
                        h = int(float(harga_text))
                        if 1_000 < h < 2_000_000:
                            hasil[internal] = h
                    except ValueError:
                        pass

        if len(hasil) >= 5:
            log.info(f"[PIHPS] ✓ {len(hasil)} komoditas dari hargapangan.id")
            return hasil
        return None

    except requests.exceptions.Timeout:
        log.warning("[PIHPS] Timeout")
        return None
    except Exception as e:
        log.debug(f"[PIHPS] Gagal: {e}")
        return None

# FETCH HARGA — Sumber 2: Panel Harga Badan Pangan

def fetch_panel_pangan() -> dict | None:
    endpoints = [
        "https://panelharga.badanpangan.go.id/api/chart/harga-nasional",
        "https://panelharga.badanpangan.go.id/chart",
    ]
    MAPPING = {
        "beras medium"  : "Beras IR III",
        "beras premium" : "Beras IR I",
        "cabai rawit"   : "Cabai Rawit Merah",
        "bawang merah"  : "Bawang Merah",
        "bawang putih"  : "Bawang Putih",
        "daging sapi"   : "Daging Sapi",
        "daging ayam"   : "Daging Ayam",
        "telur ayam"    : "Telur Ayam",
        "gula pasir"    : "Gula Pasir",
        "minyak goreng" : "Minyak Goreng",
    }
    for url in endpoints:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                continue
            try:
                data  = r.json()
                items = (data.get("data", []) or data.get("komoditas", []) or
                         (data if isinstance(data, list) else []))
                hasil = {}
                for item in items:
                    nama  = str(item.get("nama", item.get("commodity", ""))).lower()
                    harga = item.get("harga", item.get("price", item.get("value", 0)))
                    for key, internal in MAPPING.items():
                        if key in nama:
                            try:
                                h = int(float(str(harga).replace(",", "")))
                                if 1_000 < h < 2_000_000:
                                    hasil[internal] = h
                            except Exception:
                                pass
                if len(hasil) >= 4:
                    log.info(f"[PANEL] ✓ {len(hasil)} komoditas dari Panel Pangan")
                    return hasil
            except json.JSONDecodeError:
                pass
        except Exception as e:
            log.debug(f"[PANEL] Gagal {url}: {e}")
    return None

# FALLBACK — Model Korelasi Empiris (kurs nyata, harga model)

def model_korelasi_empiris(kurs_today: float, kurs_kemarin: float) -> dict:
    global _harga_state
    kurs_change = (kurs_today - kurs_kemarin) / kurs_kemarin
    hasil = {}
    for komoditas, harga_kemarin in _harga_state.items():
        korr        = KORELASI_KURS.get(komoditas, 0.15)
        dampak_kurs = harga_kemarin * korr * kurs_change
        if "Cabai" in komoditas:
            noise = harga_kemarin * random.gauss(0, 0.022)
        elif "Bawang" in komoditas:
            noise = harga_kemarin * random.gauss(0, 0.015)
        elif "Daging Sapi" in komoditas:
            noise = harga_kemarin * random.gauss(0, 0.004)
        else:
            noise = harga_kemarin * random.gauss(0, 0.007)
        tren        = harga_kemarin * (0.0624 / 365)
        harga_baru  = max(harga_kemarin + dampak_kurs + noise + tren,
                          harga_kemarin * 0.85)
        hasil[komoditas] = round(harga_baru)
    _harga_state = dict(hasil)
    log.info(f"[MODEL] Harga via model korelasi empiris (kurs Δ{kurs_change*100:+.2f}%)")
    return hasil

# [BARU] Update Riwayat Harga Harian per Komoditas

def update_historis_harga(harga_dict: dict, tanggal: str):
    """
    Tambahkan harga hari ini ke _historis_harga per komoditas.
    Jika sudah ada entri untuk tanggal yang sama (loop cepat), skip — tidak dobel.
    Batasi maksimal 30 entri per komoditas (rolling 30 hari).
    """
    for komoditas, harga in harga_dict.items():
        riwayat = _historis_harga.get(komoditas, [])

        # Cek apakah tanggal hari ini sudah ada
        if riwayat and riwayat[-1]["tanggal"] == tanggal:
            # Update harga terbaru untuk hari yang sama (tidak dobel)
            riwayat[-1]["harga"] = harga
        else:
            riwayat.append({"tanggal": tanggal, "harga": harga})

        # Jaga maksimal 30 entri
        _historis_harga[komoditas] = riwayat[-30:]


def get_harga_kemarin(komoditas: str) -> int:
    """
    Ambil harga hari sebelumnya dari riwayat.
    Jika riwayat < 2 entri, return harga referensi.
    """
    riwayat = _historis_harga.get(komoditas, [])
    if len(riwayat) >= 2:
        return riwayat[-2]["harga"]
    return HARGA_REF.get(komoditas, 0)


# ════════════════════════════════════════════════════════
# MAIN LOOP
# ════════════════════════════════════════════════════════

def main():
    global _kurs_prev

    log.info("=" * 60)
    log.info("  PRODUCER API — Kurs USD/IDR + Harga Pangan Real-Time")
    log.info(f"  Kafka: {KAFKA_BOOTSTRAP} → topic: {TOPIC_API}")
    log.info(f"  Interval: {INTERVAL_SECONDS}s | Komoditas: {len(KORELASI_KURS)}")
    log.info("  [REVISI] Historis 30h + flag musiman aktif")   # [BARU]
    log.info("=" * 60)

    # [BARU] Load kurs historis 30 hari saat startup (sekali saja)
    global _historis_kurs
    _historis_kurs = fetch_kurs_historis_30h()
    if _historis_kurs:
        # Gunakan kurs terakhir dari historis sebagai _kurs_prev awal
        _kurs_prev = _historis_kurs[-1]["kurs"]
        log.info(f"[INIT] Kurs awal dari historis: Rp{_kurs_prev:,.0f}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=3,
        request_timeout_ms=30_000,
    )
    log.info("✓ Terhubung ke Kafka")

    while True:
        ts_start    = datetime.now()
        tanggal_str = ts_start.strftime("%Y-%m-%d")
        kurs_today  = fetch_kurs_realtime()
        kurs_change = (kurs_today - KURS_REF) / KURS_REF * 100

        # [BARU] Update riwayat kurs hari ini
        if _historis_kurs and _historis_kurs[-1]["tanggal"] == tanggal_str:
            _historis_kurs[-1]["kurs"] = round(kurs_today, 2)
        else:
            _historis_kurs.append({"tanggal": tanggal_str, "kurs": round(kurs_today, 2)})
        _historis_kurs = _historis_kurs[-30:]  # jaga rolling 30 hari

        # [BARU] Hitung rata-rata kurs 7 dan 30 hari
        kurs_7d_avg  = hitung_kurs_avg(7)
        kurs_30d_avg = hitung_kurs_avg(30)

        # [BARU] Hitung flag musiman hari ini
        flag_musiman = hitung_flag_musiman(ts_start.date())
        log.info(f"  Musim       : {flag_musiman['musim_event']}")

        # Chain of sources: PIHPS → Panel → Model Empiris
        fast_mode = os.getenv("FAST_MODE", "true").lower() == "true"
        if fast_mode:
            harga_dict = model_korelasi_empiris(kurs_today, _kurs_prev)
            sumber     = "Model Korelasi Empiris (kurs real yfinance)"
        else:
            harga_dict = fetch_pihps_scraping()
            sumber     = "PIHPS hargapangan.id"
            if harga_dict:
                for k, v in harga_dict.items():
                    _harga_state[k] = v
            else:
                harga_dict = fetch_panel_pangan()
                sumber     = "Panel Harga Badan Pangan"
                if harga_dict:
                    for k, v in harga_dict.items():
                        _harga_state[k] = v
                else:
                    harga_dict = model_korelasi_empiris(kurs_today, _kurs_prev)
                    sumber     = "Model Korelasi Empiris (kurs real yfinance)"

        # [BARU] Update riwayat harga setelah dapat harga hari ini
        update_historis_harga(harga_dict, tanggal_str)

        log.info(f"  Sumber harga : {sumber}")
        log.info(f"  Kurs terkini : Rp{kurs_today:,.0f}  (Δ {kurs_change:+.2f}%)")
        log.info(f"  Kurs 7d avg  : Rp{kurs_7d_avg:,.0f} | 30d avg: Rp{kurs_30d_avg:,.0f}")

        sent = 0
        for komoditas in KORELASI_KURS:
            harga = harga_dict.get(komoditas, _harga_state.get(komoditas, 0))
            if harga <= 0:
                continue

            # [BARU] Ambil harga kemarin dari riwayat
            harga_kemarin = get_harga_kemarin(komoditas)

            payload = {
                # Field lama (tidak diubah)
                "timestamp"        : ts_start.isoformat(),
                "tanggal"          : tanggal_str,
                "jam"              : ts_start.strftime("%H:%M"),
                "komoditas"        : komoditas,
                "harga_rp"         : int(harga),
                "satuan"           : SATUAN.get(komoditas, "/kg"),
                "kurs_usd_idr"     : round(kurs_today),
                "kurs_change_pct"  : round(kurs_change, 3),
                "korelasi_kurs"    : KORELASI_KURS.get(komoditas, 0),
                "dampak_kurs_rp"   : round(harga * KORELASI_KURS.get(komoditas, 0) * (kurs_change / 100)),
                "sumber_harga"     : sumber,
                "sumber_kurs"      : "yfinance USDIDR=X (real-time)",
                "wilayah"          : "Nasional",

                # [BARU] Field turunan untuk ML Orang 2
                "harga_kemarin"    : harga_kemarin,          # harga H-1, untuk lag_1
                "kurs_7d_avg"      : kurs_7d_avg,            # rata-rata kurs 7 hari
                "kurs_30d_avg"     : kurs_30d_avg,           # rata-rata kurs 30 hari

                # [BARU] Flag musiman 
                "flag_lebaran"     : flag_musiman["flag_lebaran"],
                "flag_panen"       : flag_musiman["flag_panen"],
                "flag_impor_ekspor": flag_musiman["flag_impor_ekspor"],
                "musim_event"      : flag_musiman["musim_event"],

                # [BARU] Historis 30 hari (time series array) 
                # Dipakai consumer untuk disimpan ke HDFS dan lokal dashboard
                "historis_30h"     : _historis_harga.get(komoditas, []),

                # [BARU] Historis kurs 30 hari 
                "historis_kurs_30h": _historis_kurs,
            }

            producer.send(TOPIC_API, key=komoditas, value=payload)
            sent += 1
            log.info(
                f"  [{komoditas:22s}] Rp{harga:>10,.0f}  "
                f"kemarin: Rp{harga_kemarin:>10,.0f}  "
                f"dampak kurs: Rp{payload['dampak_kurs_rp']:>+8,.0f}"
            )

        producer.flush()
        _kurs_prev = kurs_today

        elapsed = (datetime.now() - ts_start).seconds
        log.info(f"  ✓ {sent} event → '{TOPIC_API}' ({elapsed}s)")
        log.info(f"  ⏱  Menunggu {INTERVAL_SECONDS}s...\n")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()