#!/usr/bin/env python3
"""
FLASK DASHBOARD — Big Data Analytics Harga Pangan
===================================================
Endpoint:
  GET /              → halaman utama dashboard
  GET /api/data      → semua data (Spark + live harga + RSS)
  GET /api/kurs      → kurs USD/IDR real-time (yfinance)
  GET /api/harga     → harga pangan terkini
  GET /api/berita    → berita RSS terbaru
  GET /api/spark     → hasil analisis Spark
  GET /api/health    → cek status semua komponen

Jalankan:
  python dashboard/app.py
"""

import json, logging, os
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, render_template, abort
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [FLASK] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Cache kurs agar tidak spam yfinance (update tiap 5 menit)
_kurs_cache = {"nilai": None, "updated_at": None}


def _load_json(filename: str) -> dict | list | None:
    path = DATA_DIR / filename
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def _fetch_kurs_live() -> dict:
    """Ambil kurs USD/IDR dari yfinance, cache 5 menit."""
    now    = datetime.now()
    cached = _kurs_cache
    if (cached["nilai"] and cached["updated_at"] and
            (now - cached["updated_at"]).seconds < 300):
        return {"nilai": cached["nilai"], "sumber": "cache", "updated_at": cached["updated_at"].isoformat()}

    try:
        ticker   = yf.Ticker("USDIDR=X")
        hist     = ticker.history(period="2d", interval="1h")
        if hist.empty:
            raise ValueError("Data kosong")
        kurs = float(hist["Close"].iloc[-1])
        _kurs_cache["nilai"]      = round(kurs)
        _kurs_cache["updated_at"] = now
        log.info(f"Kurs live: Rp{kurs:,.0f}")
        return {"nilai": round(kurs), "sumber": "yfinance USDIDR=X", "updated_at": now.isoformat()}
    except Exception as e:
        log.warning(f"yfinance gagal: {e}")
        fallback = _kurs_cache["nilai"] or 18_100
        return {"nilai": fallback, "sumber": "cache/fallback", "updated_at": now.isoformat()}


# ════════════════════════════════════════════════════════
# ROUTES
# ════════════════════════════════════════════════════════
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/kurs")
def api_kurs():
    return jsonify(_fetch_kurs_live())


@app.route("/api/harga")
def api_harga():
    summary = _load_json("live_summary.json")
    if not summary:
        return jsonify({"error": "Data harga belum tersedia. Jalankan producer + consumer."}), 503
    return jsonify(summary)


@app.route("/api/berita")
def api_berita():
    rss = _load_json("live_rss.json")
    if not rss:
        return jsonify([])
    # Urutkan by timestamp, ambil 10 terbaru
    rss_sorted = sorted(rss, key=lambda x: x.get("timestamp", ""), reverse=True)[:10]
    return jsonify(rss_sorted)


@app.route("/api/spark")
def api_spark():
    spark = _load_json("spark_results.json")
    if not spark:
        return jsonify({"error": "Hasil Spark belum tersedia. Jalankan spark/analysis.py."}), 503
    return jsonify(spark)


@app.route("/api/data")
def api_data():
    """Endpoint utama — gabung semua data untuk dashboard."""
    kurs_data    = _fetch_kurs_live()
    harga_data   = _load_json("live_summary.json") or {}
    berita_data  = _load_json("live_rss.json") or []
    spark_data   = _load_json("spark_results.json") or {}

    berita_sorted = sorted(berita_data, key=lambda x: x.get("timestamp", ""), reverse=True)[:10]

    return jsonify({
        "timestamp"       : datetime.now().isoformat(),
        "kurs"            : kurs_data,
        "harga_pangan"    : harga_data.get("data", {}),
        "harga_updated_at": harga_data.get("updated_at"),
        "berita"          : berita_sorted,
        "spark"           : spark_data,
    })


@app.route("/api/health")
def api_health():
    return jsonify({
        "status"      : "ok",
        "timestamp"   : datetime.now().isoformat(),
        "files"       : {f: (DATA_DIR / f).exists() for f in
                         ["live_summary.json", "live_rss.json", "spark_results.json"]},
    })


if __name__ == "__main__":
    log.info("=" * 50)
    log.info("  FLASK DASHBOARD — Pangan Big Data Analytics")
    log.info("  http://localhost:5000")
    log.info("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=False)
