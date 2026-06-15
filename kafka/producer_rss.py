#!/usr/bin/env python3
"""
KAFKA PRODUCER — Berita Ekonomi RSS Real-Time
==============================================
Sumber: Bisnis.com, Kompas, Detik Finance, CNBC Indonesia, Antara
Output: Kafka topic 'pangan-rss'

Jalankan:
  python kafka/producer_rss.py
"""

import json, time, logging, hashlib, os
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
    }
]

import random

def generate_mock_article() -> dict:
    item = random.choice(MOCK_ARTICLES)
    now = datetime.now()
    url = f"https://mocknews.co/berita/pangan/{hashlib.md5(item['judul'].encode()).hexdigest()[:8]}_{now.strftime('%H%M%S')}"
    tag = tag_relevansi(item["judul"], item["summary"])
    return {
        "id"        : hash_url(url),
        "url"       : url,
        "judul"     : f"[SIMULASI] {item['judul']}",
        "summary"   : item["summary"],
        "sumber"    : item["sumber"],
        "published" : now.strftime("%a, %d %b %Y %H:%M:%S +0700"),
        "timestamp" : now.isoformat(),
        **tag,
    }


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


def fetch_rss(feed: dict) -> list:
    try:
        parsed   = feedparser.parse(feed["url"])
        artikel  = []
        for entry in parsed.entries[:20]:
            url = entry.get("link", "")
            if not url or url in _sent_urls:
                continue
            judul     = entry.get("title", "")
            summary   = entry.get("summary", entry.get("description", ""))
            published = entry.get("published", datetime.now().isoformat())
            tag       = tag_relevansi(judul, summary)
            artikel.append({
                "id"        : hash_url(url),
                "url"       : url,
                "judul"     : judul[:300],
                "summary"   : summary[:500],
                "sumber"    : feed["nama"],
                "published" : published,
                "timestamp" : datetime.now().isoformat(),
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
                rel = "✓ RELEVAN" if (art["relevan_pangan"] or art["relevan_kurs"]) else "  biasa  "
                log.info(f"  [{feed['nama'][:15]:15s}] {rel} | {art['judul'][:55]}")

        # Hanya kirim berita asli dari RSS feeds (tidak ada lagi injeksi berita simulasi)

        producer.flush()
        log.info(f"  ✓ {total_sent} artikel terkirim | {len(_sent_urls)} total unik")
        log.info(f"  ⏱  Menunggu {INTERVAL}s...\n")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
