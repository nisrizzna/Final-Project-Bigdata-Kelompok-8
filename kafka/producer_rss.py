#!/usr/bin/env python3
"""
KAFKA PRODUCER — Berita Ekonomi RSS Real-Time
==============================================
Sumber: Bisnis.com, Kompas, Detik Finance, CNBC Indonesia, Antara
Output: Kafka topic 'pangan-rss'

Tambahan fitur:
  - Sentiment score per artikel (+1 positif / 0 netral / -1 negatif)
  - Label penyebab dominan: "kurs" / "cuaca" / "kebijakan" / "pasokan" / "umum"
  - Field sentiment_score dan sentiment_label ditambah ke setiap payload

Jalankan:
  python kafka/producer_rss.py
"""

import json, time, logging, hashlib, os, random
from datetime import datetime
from kafka import KafkaProducer
import feedparser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER-RSS] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RSS       = "pangan-rss"
INTERVAL        = 60    # 1 menit untuk kelancaran demo

RSS_FEEDS = [
    {"url": "https://rss.bisnis.com/feed/rss2/ekonomi",           "nama": "Bisnis.com"},
    {"url": "https://rss.kompas.com/feed/kompas.com/money",       "nama": "Kompas Money"},
    {"url": "https://www.cnbcindonesia.com/rss",                   "nama": "CNBC Indonesia"},
    {"url": "https://finance.detik.com/rss.xml",                   "nama": "Detik Finance"},
    {"url": "https://www.antaranews.com/rss/berita/ekonomi",       "nama": "Antara Ekonomi"},
]

KEYWORDS_PANGAN = [
    "beras", "cabai", "bawang", "daging", "telur", "gula",
    "minyak goreng", "pangan", "inflasi", "komoditas", "bapanas", "bulog",
]
KEYWORDS_KURS = [
    "dolar", "rupiah", "kurs", "dollar", "forex",
    "nilai tukar", "bi rate", "bank indonesia", "depresiasi",
]

# [BARU] Keyword untuk Sentiment Scoring
# Kata-kata yang menandakan kondisi NEGATIF → harga cenderung naik
KEYWORDS_NEGATIF = [
    "gagal panen", "kelangkaan", "embargo", "kenaikan harga", "lonjakan harga",
    "harga melonjak", "harga naik", "harga melambung", "cuaca ekstrem",
    "banjir", "kekeringan", "hama", "puso", "defisit", "mahal",
    "langka", "krisis pangan", "impor terhambat", "pasokan terganggu",
    "inflasi tinggi", "depresiasi rupiah", "rupiah melemah", "dolar menguat",
]

# Kata-kata yang menandakan kondisi POSITIF → harga cenderung stabil/turun
KEYWORDS_POSITIF = [
    "panen raya", "stok aman", "subsidi", "impor masuk", "harga turun",
    "harga stabil", "pasokan melimpah", "surplus", "cadangan cukup",
    "operasi pasar", "bulog menjamin", "ketahanan pangan", "panen melimpah",
    "produksi meningkat", "ekspor meningkat", "rupiah menguat", "dolar melemah",
    "harga terjangkau", "stabilisasi harga", "intervensi pemerintah",
]

# Keyword untuk mendeteksi PENYEBAB dominan dari berita
KEYWORDS_PENYEBAB = {
    "kurs"      : ["dolar", "kurs", "rupiah", "depresiasi", "forex", "nilai tukar", "dollar"],
    "cuaca"     : ["cuaca", "banjir", "kekeringan", "hujan", "el nino", "la nina", "iklim", "kemarau"],
    "kebijakan" : ["kebijakan", "impor", "ekspor", "subsidi", "tarif", "regulasi", "pemerintah", "kemendag", "bapanas"],
    "pasokan"   : ["stok", "pasokan", "distribusi", "logistik", "gudang", "rantai pasok", "bulog", "kelangkaan"],
}

_sent_urls: set = set()

MOCK_ARTICLES = [
    {
        "judul": "Bulog Pastikan Stok Beras Aman Menjelang Libur Panjang",
        "summary": "Direktur Utama Bulog menyatakan pasokan beras nasional aman dan cadangan pangan di gudang mencukupi kebutuhan masyarakat.",
        "sumber": "Bulog News"
    },
    {
        "judul": "Harga Cabai Rawit Merah Melonjak akibat Cuaca Ekstrem di Sentra Tani",
        "summary": "Petani di berbagai daerah melaporkan gagal panen cabai rawit merah karena curah hujan tinggi, memicu kenaikan harga pangan.",
        "sumber": "Antara Ekonomi"
    },
    {
        "judul": "Kementerian Perdagangan Monitor Distribusi Minyak Goreng Kita",
        "summary": "Upaya pengawasan distribusi minyak goreng curah terus ditingkatkan untuk mencegah kelangkaan dan menjaga stabilitas harga di pasar.",
        "sumber": "Detik Finance"
    },
    {
        "judul": "Impor Bawang Putih Masuk, Stabilitas Harga Diharapkan Terpelihara",
        "summary": "Pemerintah merealisasikan impor bawang putih guna mengantisipasi defisit pasokan di pasar tradisional dalam beberapa bulan ke depan.",
        "sumber": "CNBC Indonesia"
    },
    {
        "judul": "Harga Daging Sapi Stabil di Kisaran Rp155.000 per Kilogram",
        "summary": "Asosiasi Pedagang Daging menyatakan pasokan daging sapi segar aman dan harga di pasar tradisional Jabodetabek terpantau stabil.",
        "sumber": "Kompas Money"
    },
    {
        "judul": "Pasokan Telur Ayam Melimpah, Peternak Keluhkan Harga Turun Tajam",
        "summary": "Melimpahnya pasokan telur ayam ras di tingkat peternak menyebabkan harga jual jatuh bebas di bawah harga acuan pemerintah.",
        "sumber": "Bisnis.com"
    },
    {
        "judul": "Bapanas Dorong Diversifikasi Pangan Berbasis Komoditas Lokal",
        "summary": "Badan Pangan Nasional (Bapanas) mengajak masyarakat mengurangi ketergantungan pada beras dan beralih ke sumber karbohidrat alternatif.",
        "sumber": "Bapanas Media"
    },
    {
        "judul": "Harga Bawang Merah Turun Drastis di Sentra Produksi Utama",
        "summary": "Memasuki musim panen raya, harga bawang merah di tingkat petani merosot tajam akibat pasokan melimpah ruah di pasar tradisional.",
        "sumber": "Antara Ekonomi"
    },
    {
        "judul": "Dampak Inflasi Global Terhadap Harga Gula Pasir Impor",
        "summary": "Kenaikan harga gula pasir internasional mulai berdampak pada harga jual gula kristal putih di tingkat konsumen dalam negeri.",
        "sumber": "CNBC Indonesia"
    },
    {
        "judul": "Rupiah Melemah ke Rp18.200, Harga Bawang Putih Impor Terancam Naik",
        "summary": "Depresiasi rupiah terhadap dolar AS berdampak langsung pada harga komoditas impor seperti bawang putih dan daging sapi di pasar domestik.",
        "sumber": "CNBC Indonesia"
    },
    {
        "judul": "Operasi Pasar Murah Bulog Berhasil Tekan Harga Beras di 10 Kota",
        "summary": "Intervensi pemerintah melalui operasi pasar murah terbukti efektif menstabilkan harga beras premium di sejumlah kota besar Indonesia.",
        "sumber": "Antara Ekonomi"
    },
]

# [BARU] Fungsi Sentiment Scoring
def hitung_sentiment(judul: str, summary: str) -> int:
    """
    Hitung sentiment score berdasarkan keyword matching.
    Return:
        +1 = sentimen positif (berita baik untuk harga, cenderung stabil/turun)
        -1 = sentimen negatif (berita buruk untuk harga, cenderung naik)
         0 = netral / tidak dapat ditentukan
    Jika ada konflik (ada keyword positif DAN negatif), negatif lebih diprioritaskan
    karena dalam konteks ketahanan pangan, berita buruk lebih kritis untuk diwaspadai.
    """
    teks = (judul + " " + summary).lower()

    ada_negatif = any(k in teks for k in KEYWORDS_NEGATIF)
    ada_positif = any(k in teks for k in KEYWORDS_POSITIF)

    if ada_negatif:
        return -1   # prioritaskan negatif (lebih kritis untuk sistem peringatan dini)
    if ada_positif:
        return +1
    return 0


def deteksi_penyebab(judul: str, summary: str) -> str:
    """
    Deteksi penyebab dominan dari isi berita.
    Berguna untuk Orang 2 (ML) dan Orang 3 (dashboard) agar bisa menampilkan
    label penyebab pada panel alert, bukan hanya skor angka.

    Return: "kurs" / "cuaca" / "kebijakan" / "pasokan" / "umum"
    """
    teks  = (judul + " " + summary).lower()
    skor  = {kategori: 0 for kategori in KEYWORDS_PENYEBAB}

    for kategori, keywords in KEYWORDS_PENYEBAB.items():
        for kw in keywords:
            if kw in teks:
                skor[kategori] += 1

    # Ambil kategori dengan skor tertinggi
    max_skor = max(skor.values())
    if max_skor == 0:
        return "umum"
    return max(skor, key=skor.get)

# Fungsi lama (tidak diubah)
def hash_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def tag_relevansi(judul: str, summary: str) -> dict:
    teks              = (judul + " " + summary).lower()
    komoditas_disebut = [k for k in KEYWORDS_PANGAN if k in teks]
    kurs_disebut      = any(k in teks for k in KEYWORDS_KURS)
    return {
        "relevan_pangan"  : len(komoditas_disebut) > 0,
        "relevan_kurs"    : kurs_disebut,
        "komoditas_tag"   : komoditas_disebut[:5],
        "skor_relevansi"  : len(komoditas_disebut) + (2 if kurs_disebut else 0),
    }


def generate_mock_article() -> dict:
    item = random.choice(MOCK_ARTICLES)
    now  = datetime.now()
    url  = f"https://mocknews.co/berita/pangan/{hashlib.md5(item['judul'].encode()).hexdigest()[:8]}_{now.strftime('%H%M%S')}"
    tag  = tag_relevansi(item["judul"], item["summary"])

    # [BARU] tambah sentiment ke mock article
    sentiment = hitung_sentiment(item["judul"], item["summary"])
    penyebab  = deteksi_penyebab(item["judul"], item["summary"])

    return {
        "id"               : hash_url(url),
        "url"              : url,
        "judul"            : f"[SIMULASI] {item['judul']}",
        "summary"          : item["summary"],
        "sumber"           : item["sumber"],
        "published"        : now.strftime("%a, %d %b %Y %H:%M:%S +0700"),
        "timestamp"        : now.isoformat(),
        "sentiment_score"  : sentiment,           # [BARU] +1 / 0 / -1
        "sentiment_label"  : (                    # [BARU] label teks untuk dashboard
            "positif" if sentiment == 1
            else "negatif" if sentiment == -1
            else "netral"
        ),
        "penyebab_dominan" : penyebab,            # [BARU] "kurs"/"cuaca"/"kebijakan"/"pasokan"/"umum"
        **tag,
    }


def fetch_rss(feed: dict) -> list:
    try:
        parsed  = feedparser.parse(feed["url"])
        artikel = []
        for entry in parsed.entries[:20]:
            url = entry.get("link", "")
            if not url or url in _sent_urls:
                continue
            judul     = entry.get("title", "")
            summary   = entry.get("summary", entry.get("description", ""))
            published = entry.get("published", datetime.now().isoformat())
            tag       = tag_relevansi(judul, summary)

            # [BARU] hitung sentiment dan penyebab sebelum kirim ke Kafka
            sentiment = hitung_sentiment(judul, summary)
            penyebab  = deteksi_penyebab(judul, summary)

            artikel.append({
                "id"               : hash_url(url),
                "url"              : url,
                "judul"            : judul[:300],
                "summary"          : summary[:500],
                "sumber"           : feed["nama"],
                "published"        : published,
                "timestamp"        : datetime.now().isoformat(),
                "sentiment_score"  : sentiment,       # [BARU] +1 / 0 / -1
                "sentiment_label"  : (                # [BARU] label untuk dashboard
                    "positif" if sentiment == 1
                    else "negatif" if sentiment == -1
                    else "netral"
                ),
                "penyebab_dominan" : penyebab,        # [BARU] kategori penyebab
                **tag,
            })
        return artikel
    except Exception as e:
        log.warning(f"[{feed['nama']}] Gagal fetch: {e}")
        return []


def main():
    log.info("=" * 55)
    log.info("  PRODUCER RSS — Berita Ekonomi Indonesia Real-Time")
    log.info(f"  Kafka: {KAFKA_BOOTSTRAP} | Topic: {TOPIC_RSS}")
    log.info(f"  Feed: {len(RSS_FEEDS)} sumber | Interval: {INTERVAL}s")
    log.info("  [REVISI] Sentiment scoring aktif (+1/0/-1)")   # [BARU]
    log.info("=" * 55)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=3,
    )
    log.info("✓ Terhubung ke Kafka")

    while True:
        total_sent = 0
        for feed in RSS_FEEDS:
            for art in fetch_rss(feed):
                producer.send(TOPIC_RSS, key=art["id"], value=art)
                _sent_urls.add(art["url"])
                total_sent += 1

                # [DIUPDATE] log sekarang tampilkan sentiment score juga
                rel       = "✓ RELEVAN" if (art["relevan_pangan"] or art["relevan_kurs"]) else "  biasa  "
                skor_icon = "📈" if art["sentiment_score"] == 1 else ("📉" if art["sentiment_score"] == -1 else "➖")
                log.info(
                    f"  [{feed['nama'][:15]:15s}] {rel} {skor_icon} "
                    f"[{art['sentiment_label']:8s}|{art['penyebab_dominan']:10s}] "
                    f"{art['judul'][:45]}"
                )

        producer.flush()
        log.info(f"  ✓ {total_sent} artikel terkirim | {len(_sent_urls)} total unik")
        log.info(f"  ⏱  Menunggu {INTERVAL}s...\n")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()