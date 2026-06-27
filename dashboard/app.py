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
  GET /api/rekomendasi → rekomendasi LLM berbasis alert prediksi [BARU]
  GET /api/timeseries/<slug> → data time series 30 hari per komoditas [BARU]

Jalankan:
  python dashboard/app.py
"""

import json, logging, os, time, requests
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, render_template, abort
from flask_cors import CORS
import yfinance as yf

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [FLASK] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# ─── Cache ───────────────────────────────────────────────────
_kurs_cache        = {"nilai": None, "updated_at": None}
_rekomendasi_cache = {"data": None, "updated_at": None}
REKOMENDASI_TTL    = 1800   # 30 menit


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
        ticker = yf.Ticker("USDIDR=X")
        hist   = ticker.history(period="2d", interval="1h")
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
# LLM INTEGRATION — Claude → Gemini → Rule-Based Fallback
# ════════════════════════════════════════════════════════

PENYEBAB_LABEL = {
    "kurs":      "depresiasi rupiah terhadap USD",
    "cuaca":     "anomali cuaca / bencana alam",
    "kebijakan": "perubahan kebijakan impor/ekspor",
    "pasokan":   "gangguan rantai pasokan",
    "musiman":   "faktor musiman (lebaran/panen/dll)",
    "lainnya":   "faktor eksternal tidak teridentifikasi",
}

INTERVENSI_TEMPLATE = {
    "Beras":       "percepat realisasi impor beras, aktifkan operasi pasar Bulog",
    "Jagung":      "stabilkan harga pakan ternak, koordinasi dengan Bulog",
    "Kedelai":     "dorong impor kedelai, subsidi petani lokal",
    "Daging Sapi": "buka keran impor sapi bakalan, kendalikan RPH swasta",
    "Daging Ayam": "stabilkan harga DOC, subsidi pakan unggas",
    "Telur Ayam":  "intervensi harga pakan, jaga rantai distribusi",
    "Minyak":      "pantau distribusi minyak goreng, tindak penimbunan",
    "Gula":        "tambah kuota impor raw sugar, libatkan PTPN",
    "Bawang":      "percepat realisasi impor, dorong off-season farming",
    "Cabai":       "stabilisasi distribusi, dukung greenhouse farming",
    "Tepung":      "jaga impor gandum, subsidi tepung industri UMKM",
}


def _build_prompt(alerts_aktif: list, berita_relevan: dict, kurs: int) -> str:
    """Susun prompt ke LLM berdasarkan alert aktif dan berita relevan."""
    lines = [
        "Kamu adalah analis kebijakan pangan Indonesia.",
        f"Kurs USD/IDR saat ini: Rp{kurs:,}.",
        "",
        "Berikut komoditas yang diprediksi melewati batas aman dalam 7–30 hari ke depan:",
        "",
    ]
    for a in alerts_aktif:
        pct_naik = round((a['prediksi'] - a['harga_saat_ini']) / max(a['harga_saat_ini'], 1) * 100, 1)
        lines.append(
            f"• {a['komoditas']} ({a['status']}): harga saat ini Rp{a['harga_saat_ini']:,}, "
            f"prediksi hari ke-{a['hari_ke']}: Rp{a['prediksi']:,} (+{pct_naik}%)"
        )
        brt = berita_relevan.get(a['komoditas'], [])
        if brt:
            penyebab = brt[0].get('penyebab_dominan', 'lainnya')
            lines.append(f"  Penyebab terdeteksi: {PENYEBAB_LABEL.get(penyebab, penyebab)}")
            for b in brt[:2]:
                lines.append(f"  - [Berita] {b.get('judul','')}")
        lines.append("")

    lines += [
        "Berikan analisis singkat (3–5 kalimat) dan rekomendasi intervensi kebijakan yang spesifik, actionable,",
        "dan terukur untuk masing-masing komoditas yang berstatus WASPADA atau BAHAYA.",
        "Tulis dalam Bahasa Indonesia yang formal dan ringkas.",
        "Format: untuk setiap komoditas, tulis satu paragraf terpisah.",
    ]
    return "\n".join(lines)


def _call_claude(prompt: str) -> str | None:
    """Panggil Claude API (claude-sonnet-4-6). Return teks atau None jika gagal."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            return data["content"][0]["text"].strip()
        log.warning(f"Claude API error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.warning(f"Claude gagal: {e}")
    return None


def _call_gemini(prompt: str) -> str | None:
    """Fallback ke Gemini API. Return teks atau None jika gagal."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        r = requests.post(
            url,
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        log.warning(f"Gemini API error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.warning(f"Gemini gagal: {e}")
    return None


def _rule_based_rekomendasi(alerts_aktif: list, berita_relevan: dict, kurs: int) -> str:
    """Rule-based fallback — tidak perlu API key, selalu berhasil."""
    if not alerts_aktif:
        return "Semua komoditas strategis dalam kondisi AMAN. Tidak diperlukan intervensi mendesak saat ini."

    paragraphs = [
        f"ANALISIS OTOMATIS — Kurs USD/IDR: Rp{kurs:,}\n"
    ]
    for a in alerts_aktif:
        pct  = round((a['prediksi'] - a['harga_saat_ini']) / max(a['harga_saat_ini'], 1) * 100, 1)
        brt  = berita_relevan.get(a['komoditas'], [])
        penyebab_raw = brt[0].get('penyebab_dominan', 'lainnya') if brt else 'lainnya'
        penyebab     = PENYEBAB_LABEL.get(penyebab_raw, penyebab_raw)
        n_berita     = len(brt)

        # exact match dulu, baru partial match — hindari "Daging Sapi" match ke "Daging Ayam"
        komoditas_key = next((k for k in INTERVENSI_TEMPLATE if k.lower() == a['komoditas'].lower()), None) or \
                        next((k for k in INTERVENSI_TEMPLATE if k.lower() in a['komoditas'].lower()), None)
        intervensi    = INTERVENSI_TEMPLATE.get(komoditas_key, "lakukan operasi pasar dan koordinasi lintas K/L terkait")

        em = "BAHAYA" if a['status'] == "BAHAYA" else "WASPADA"
        paragraphs.append(
            f"[{em}] {a['komoditas']}\n"
            f"Harga saat ini: Rp{a['harga_saat_ini']:,} → Prediksi hari ke-{a['hari_ke']}: Rp{a['prediksi']:,} ({'+' if pct>=0 else ''}{pct}%)\n"
            f"Penyebab: {penyebab}"
            + (f" ({n_berita} artikel berita terkait)" if n_berita else "") + "\n"
            f"Rekomendasi: {intervensi}."
        )
    return "\n\n".join(paragraphs)


def _generate_rekomendasi() -> dict:
    """Orkestrasi: baca data → build prompt → Claude → Gemini → rule-based."""
    spark  = _load_json("spark_results.json") or {}
    rss    = _load_json("live_rss.json") or []
    kurs   = _kurs_cache.get("nilai") or 18_100

    prediksi_list = spark.get("prediksi", [])

    # Kumpulkan alert — 1 entri per komoditas, ambil horizon terpendek yang alert
    alerts_aktif = []
    for item in prediksi_list:
        komoditas    = item.get("komoditas", "")
        harga_skrg   = item.get("harga_saat_ini", 0)
        alerts_spark = item.get("alerts", {})
        for key in ("t7", "t14", "t30"):
            al = alerts_spark.get(key, {})
            if al.get("status") in ("WASPADA", "BAHAYA"):
                alerts_aktif.append({
                    "komoditas":      komoditas,
                    "status":         al["status"],
                    "hari_ke":        al["hari_ke"],
                    "prediksi":       round(al["prediksi"]),
                    "harga_saat_ini": round(harga_skrg),
                    "threshold":      round(al.get("threshold_waspada", 0)),
                })
                break  # ambil horizon terpendek saja, stop
    alerts_aktif = alerts_aktif[:7]  # maksimal 7 komoditas di prompt

    # Kumpulkan berita relevan per komoditas
    berita_relevan = {}
    for a in alerts_aktif:
        nama_lower = a["komoditas"].lower()
        cocok = [
            b for b in rss
            if nama_lower in str(b.get("komoditas_tag", [])).lower()
            or nama_lower in b.get("judul", "").lower()
        ][:5]
        berita_relevan[a["komoditas"]] = cocok

    # Tentukan sumber LLM
    if not alerts_aktif:
        teks   = "✅ Semua komoditas strategis saat ini berstatus AMAN berdasarkan prediksi Spark MLlib. Tidak diperlukan intervensi kebijakan mendesak."
        sumber = "rule-based"
    else:
        prompt = _build_prompt(alerts_aktif, berita_relevan, kurs)
        teks   = _call_claude(prompt)
        sumber = "Claude (claude-sonnet-4-6)"
        if not teks:
            teks   = _call_gemini(prompt)
            sumber = "Gemini (gemini-2.0-flash)"
        if not teks:
            teks   = _rule_based_rekomendasi(alerts_aktif, berita_relevan, kurs)
            sumber = "rule-based (offline)"

    log.info(f"Rekomendasi dihasilkan via {sumber} — {len(alerts_aktif)} alert aktif")
    return {
        "rekomendasi":  teks,
        "sumber_llm":   sumber,
        "jumlah_alert": len(alerts_aktif),
        "alerts_aktif": alerts_aktif,
        "generated_at": datetime.now().isoformat(),
    }


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
    rss_sorted = sorted(rss, key=lambda x: x.get("timestamp", ""), reverse=True)[:10]
    return jsonify(rss_sorted)


@app.route("/api/spark")
def api_spark():
    spark = _load_json("spark_results.json")
    if not spark:
        return jsonify({"error": "Hasil Spark belum tersedia. Jalankan spark/analysis.py."}), 503
    return jsonify(spark)


@app.route("/api/rekomendasi")
def api_rekomendasi():
    """
    Endpoint LLM — generate rekomendasi intervensi kebijakan pangan.
    Cache 30 menit. Fallback otomatis: Claude → Gemini → rule-based.
    """
    now    = time.time()
    cached = _rekomendasi_cache
    if (cached["data"] and cached["updated_at"] and
            (now - cached["updated_at"]) < REKOMENDASI_TTL):
        result = cached["data"].copy()
        result["cached"] = True
        return jsonify(result)

    result = _generate_rekomendasi()
    _rekomendasi_cache["data"]       = result
    _rekomendasi_cache["updated_at"] = now
    result["cached"] = False
    return jsonify(result)


@app.route("/api/timeseries/<slug>")
def api_timeseries(slug: str):
    """
    Data time series 30 hari per komoditas.
    Slug: nama komoditas lowercase, spasi → underscore, e.g. beras_ir_i
    """
    # sanitasi slug
    slug = "".join(c for c in slug.lower().replace(" ", "_") if c.isalnum() or c == "_")
    data = _load_json(f"timeseries_{slug}.json")
    if data is None:
        return jsonify([])
    return jsonify(data)


@app.route("/api/data")
def api_data():
    """Endpoint utama — gabung semua data untuk dashboard."""
    kurs_data   = _fetch_kurs_live()
    harga_data  = _load_json("live_summary.json") or {}
    berita_data = _load_json("live_rss.json") or []
    spark_data  = _load_json("spark_results.json") or {}

    berita_sorted = sorted(berita_data, key=lambda x: x.get("timestamp", ""), reverse=True)[:10]

    return jsonify({
        "timestamp":        datetime.now().isoformat(),
        "kurs":             kurs_data,
        "harga_pangan":     harga_data.get("data", {}),
        "harga_updated_at": harga_data.get("updated_at"),
        "berita":           berita_sorted,
        "spark":            spark_data,
    })


@app.route("/api/health")
def api_health():
    return jsonify({
        "status":    "ok",
        "timestamp": datetime.now().isoformat(),
        "files":     {f: (DATA_DIR / f).exists() for f in
                      ["live_summary.json", "live_rss.json", "spark_results.json"]},
        "llm": {
            "claude_key_set":  bool(os.environ.get("ANTHROPIC_API_KEY")),
            "gemini_key_set":  bool(os.environ.get("GEMINI_API_KEY")),
            "cache_valid":     bool(_rekomendasi_cache["data"]),
        },
    })


if __name__ == "__main__":
    log.info("=" * 55)
    log.info("  FLASK DASHBOARD — Pangan Big Data Analytics")
    log.info("  http://localhost:5000")
    log.info(f"  Claude key : {'✓ SET' if os.environ.get('ANTHROPIC_API_KEY') else '✗ tidak ada'}")
    log.info(f"  Gemini key : {'✓ SET' if os.environ.get('GEMINI_API_KEY') else '✗ tidak ada'}")
    log.info("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False)