#!/usr/bin/env python3
"""
FLASK DASHBOARD — Big Data Analytics Harga Pangan
===================================================
Endpoint:
  GET /                  → halaman utama dashboard
  GET /api/data          → semua data (Spark + live harga + RSS)
  GET /api/kurs          → kurs USD/IDR real-time (yfinance)
  GET /api/harga         → harga pangan terkini
  GET /api/berita        → berita RSS terbaru
  GET /api/spark         → hasil analisis Spark
  GET /api/timeseries    → time series 30 hari per komoditas
  GET /api/rekomendasi   → rekomendasi intervensi dari LLM (Claude/Gemini/fallback)
  GET /api/health        → cek status semua komponen

Env vars:
  ANTHROPIC_API_KEY  — Claude API key (primary LLM)
  GEMINI_API_KEY     — Gemini API key (fallback LLM)
"""

import json, logging, os, time
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, render_template, abort, Response
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
import yfinance as yf
import requests

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

# ── Pastikan semua error di /api/* selalu return JSON, bukan HTML ──
@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return jsonify({"error": e.description, "code": e.code}), e.code
    log.error(f"Unhandled: {e}")
    return jsonify({"error": str(e)}), 500

# ── Cache kurs (5 menit) ──────────────────────────────────────
_kurs_cache = {"nilai": None, "updated_at": None}

# ── Cache rekomendasi LLM (30 menit) ─────────────────────────
_rek_cache: dict = {"text": None, "ts": 0}
REK_TTL = 30 * 60  # detik


# ════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════

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
# REKOMENDASI LLM
# ════════════════════════════════════════════════════════

def _build_prompt(alerts: list, spark: dict, berita_list: list) -> str:
    """Susun prompt untuk LLM berisi konteks alert + berita + prediksi."""
    if not alerts:
        return ""

    # Ambil prediksi per komoditas yang alert
    prediksi_map: dict = {}
    for p in spark.get("prediksi", []):
        prediksi_map[p["komoditas"]] = p

    # Kumpulkan berita per komoditas alert (maks 4 per komoditas)
    alert_komoditas = {a["komoditas"] for a in alerts}
    berita_relevan: dict[str, list] = {k: [] for k in alert_komoditas}
    for b in sorted(berita_list, key=lambda x: x.get("timestamp", ""), reverse=True):
        tags = b.get("komoditas_tag", [])
        if isinstance(tags, str):
            tags = [tags]
        for k in alert_komoditas:
            if any(k.lower() in (t.lower() if isinstance(t, str) else "") for t in tags) \
               or k.lower() in b.get("judul", "").lower():
                if len(berita_relevan[k]) < 4:
                    berita_relevan[k].append(b)

    lines = [
        "Kamu adalah analis kebijakan pangan senior Indonesia.",
        "Berdasarkan data Spark Big Data berikut, berikan rekomendasi intervensi pemerintah yang SPESIFIK dan ACTIONABLE.",
        "",
        f"Tanggal analisis: {datetime.now().strftime('%d %B %Y %H:%M WIB')}",
        "",
        "=== ALERT KOMODITAS BERMASALAH ===",
    ]

    for a in alerts:
        k = a["komoditas"]
        p = prediksi_map.get(k, {})
        pct_kenaikan = 0
        if p.get("harga_saat_ini", 0) > 0 and a.get("prediksi", 0) > 0:
            pct_kenaikan = ((a["prediksi"] - p["harga_saat_ini"]) / p["harga_saat_ini"]) * 100

        lines.append(f"\n[{a['status']}] {k}")
        lines.append(f"  - Harga saat ini  : Rp{p.get('harga_saat_ini', 0):,.0f}")
        lines.append(f"  - Prediksi {a.get('hari_ke', '?')} hari : Rp{a.get('prediksi', 0):,.0f} "
                     f"(+{pct_kenaikan:.1f}%)")
        if p.get("prediksi_7h"):
            lines.append(f"  - Prediksi 7h/14h/30h: "
                         f"Rp{p.get('prediksi_7h',0):,.0f} / Rp{p.get('prediksi_14h',0):,.0f} / Rp{p.get('prediksi_30h',0):,.0f}")
        lines.append(f"  - MAE model       : {p.get('mae', 0):,.0f} | MAPE: {p.get('mape_pct', 0):.1f}%")

        berita_k = berita_relevan.get(k, [])
        if berita_k:
            lines.append(f"  - Berita terkait  :")
            for b in berita_k:
                sent  = b.get("sentiment_label", "")
                sebab = b.get("penyebab_dominan", "")
                lines.append(f"      [{sent}|{sebab}] {b.get('judul', '')[:90]}")

    kurs = spark.get("korelasi_kurs", [{}])[0].get("kurs_saat_ini", "N/A") if spark.get("korelasi_kurs") else "N/A"
    lines += [
        "",
        f"=== KONTEKS MAKRO ===",
        f"Kurs USD/IDR saat ini: Rp{kurs}",
        "",
        "=== INSTRUKSI OUTPUT ===",
        "Tulis laporan singkat dalam Bahasa Indonesia dengan format:",
        "1. Ringkasan situasi (2-3 kalimat)",
        "2. Untuk SETIAP komoditas bermasalah: analisis penyebab + rekomendasi intervensi spesifik (volume, instansi, waktu)",
        "3. Rekomendasi lintas-komoditas jika ada pola umum",
        "Gunakan angka konkret. Maksimal 400 kata.",
    ]
    return "\n".join(lines)


def _call_claude(prompt: str) -> str | None:
    """Panggil Claude API. Return teks atau None jika gagal."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None
    try:
        resp = requests.post(
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
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]
    except Exception as e:
        log.warning(f"Claude API gagal: {e}")
        return None


def _call_gemini(prompt: str) -> str | None:
    """Panggil Gemini API sebagai fallback. Return teks atau None jika gagal."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None
    try:
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={api_key}"
        )
        resp = requests.post(
            url,
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        log.warning(f"Gemini API gagal: {e}")
        return None


def _rule_based_rekomendasi(alerts: list, spark: dict) -> str:
    """
    Fallback rule-based: hasilkan rekomendasi spesifik tanpa LLM.
    Dipakai saat kedua API key kosong atau gagal.
    """
    if not alerts:
        return "Tidak ada komoditas dalam status WASPADA atau BAHAYA saat ini."

    prediksi_map: dict = {p["komoditas"]: p for p in spark.get("prediksi", [])}

    penyebab_teks = {
        "kurs":      "depresiasi nilai tukar Rupiah terhadap USD",
        "cuaca":     "anomali cuaca / gangguan iklim",
        "kebijakan": "kebijakan impor/ekspor yang belum optimal",
        "pasokan":   "gangguan rantai pasokan domestik",
        "umum":      "kombinasi tekanan makroekonomi",
    }

    intervensi_map = {
        "Beras":     "Percepat realisasi impor beras 500 ribu ton via Bulog; aktifkan Operasi Pasar di 5 provinsi defisit.",
        "Cabai":     "Dorong distribusi hortikultura lintas provinsi; aktifkan cold storage Kementan untuk mengurangi susut pasca-panen.",
        "Gula":      "Percepat penerbitan izin impor gula mentah; monitor distribusi gula BUMN ke pasar tradisional.",
        "Minyak":    "Optimalkan penyaluran minyak goreng subsidi; tindak pelaku penimbunan lewat Satgas Pangan.",
        "Daging":    "Realisasikan kuota impor daging sapi beku; intensifkan operasi pasar Bulog dan pengawasan RPH.",
        "Kedelai":   "Percepat impor kedelai untuk perajin tahu/tempe; pertimbangkan subsidi langsung 14 juta perajin.",
        "Telur":     "Stabilisasi harga pakan unggas (jagung/SBM); aktifkan intervensi harga acuan Kemendag.",
        "Jagung":    "Serap hasil panen domestik melalui Bulog; tunda izin ekspor jagung sampai stok aman.",
        "Tepung":    "Monitor stok gandum nasional; koordinasi dengan importir untuk menjaga buffer 3 bulan.",
        "Ikan":      "Fasilitasi distribusi ikan tangkap dari sentra ke daerah defisit; subsidi BBM nelayan.",
        "Ayam":      "Kendalikan harga DOC; awasi integrasi vertikal peternakan besar agar tidak menekan peternak kecil.",
    }

    lines = [
        f"📊 **Laporan Rekomendasi Intervensi Pangan — {datetime.now().strftime('%d %B %Y %H:%M WIB')}**",
        "",
        f"**Ringkasan Situasi:** Terdapat {len(alerts)} sinyal peringatan pada komoditas strategis.",
        "Sistem mendeteksi tekanan harga yang berpotensi berdampak pada daya beli masyarakat.",
        "",
    ]

    for a in alerts:
        k   = a["komoditas"]
        p   = prediksi_map.get(k, {})
        harga_saat = p.get("harga_saat_ini", 0)
        harga_pred = a.get("prediksi", 0)
        pct = ((harga_pred - harga_saat) / harga_saat * 100) if harga_saat > 0 else 0
        status_emoji = "🔴" if a["status"] == "BAHAYA" else "🟡"

        # Cari intervensi yang paling cocok
        intervensi = next(
            (v for keyword, v in intervensi_map.items() if keyword.lower() in k.lower()),
            f"Koordinasi Kemendag-Bulog untuk menjaga harga {k} di bawah HET."
        )

        lines += [
            f"{status_emoji} **{k}** [{a['status']}]",
            f"  • Prediksi {a.get('hari_ke','?')} hari ke depan: Rp{harga_pred:,.0f} "
            f"(+{pct:.1f}% dari Rp{harga_saat:,.0f})",
            f"  • Rekomendasi: {intervensi}",
            "",
        ]

    lines += [
        "**Rekomendasi Lintas-Komoditas:**",
        "• Aktifkan Satgas Pangan Nasional untuk pemantauan harian di pasar-pasar induk utama.",
        "• Koordinasi Kemenkeu untuk relaksasi tarif impor sementara pada komoditas WASPADA/BAHAYA.",
        "• Percepat penyaluran dana BPNT agar daya beli rumah tangga miskin tetap terjaga.",
    ]

    return "\n".join(lines)


def _get_rekomendasi(force_refresh: bool = False) -> dict:
    """Ambil rekomendasi: dari cache → LLM → fallback rule-based."""
    now_ts = time.time()
    if not force_refresh and _rek_cache["text"] and (now_ts - _rek_cache["ts"]) < REK_TTL:
        return {
            "rekomendasi": _rek_cache["text"],
            "sumber":      _rek_cache.get("sumber", "cache"),
            "cached_at":   datetime.fromtimestamp(_rek_cache["ts"]).isoformat(),
            "cache_ttl_s": int(REK_TTL - (now_ts - _rek_cache["ts"])),
        }

    spark      = _load_json("spark_results.json") or {}
    berita_raw = _load_json("live_rss.json") or []

    alerts = [a for a in spark.get("alerts", []) if a.get("status") in ("WASPADA", "BAHAYA")]

    if not alerts:
        teks   = "Semua komoditas saat ini dalam status AMAN. Tidak ada rekomendasi intervensi yang diperlukan."
        sumber = "no-alert"
    else:
        prompt = _build_prompt(alerts, spark, berita_raw)
        teks   = _call_claude(prompt)
        sumber = "claude"

        if not teks:
            teks   = _call_gemini(prompt)
            sumber = "gemini"

        if not teks:
            teks   = _rule_based_rekomendasi(alerts, spark)
            sumber = "rule-based"

    _rek_cache["text"]   = teks
    _rek_cache["ts"]     = now_ts
    _rek_cache["sumber"] = sumber

    return {
        "rekomendasi": teks,
        "sumber":      sumber,
        "cached_at":   datetime.fromtimestamp(now_ts).isoformat(),
        "cache_ttl_s": int(REK_TTL),
        "n_alerts":    len(alerts),
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
        return jsonify({"error": "Data harga belum tersedia.", "data": {}}), 200
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
        return jsonify({"error": "Hasil Spark belum tersedia.", "prediksi": [], "alerts": []}), 200
    return jsonify(spark)


@app.route("/api/timeseries")
def api_timeseries():
    """Kumpulkan semua file timeseries_*.json dan return sebagai dict {slug: [...]}.
    Dipakai dashboard untuk line chart historis 30 hari + prediksi ke depan.
    """
    result: dict = {}
    for ts_file in DATA_DIR.glob("timeseries_*.json"):
        slug = ts_file.stem.replace("timeseries_", "")
        data = []
        try:
            with open(ts_file, encoding="utf-8") as f:
                raw = json.load(f)
                data = raw if isinstance(raw, list) else []
        except Exception:
            pass
        result[slug] = data
    return jsonify(result)


@app.route("/api/rekomendasi")
def api_rekomendasi():
    """
    Endpoint rekomendasi intervensi dari LLM.
    Query param: ?refresh=1  → paksa generate ulang meski cache belum expired.
    """
    from flask import request as freq
    force = freq.args.get("refresh", "0") == "1"
    try:
        result = _get_rekomendasi(force_refresh=force)
        return jsonify(result)
    except Exception as e:
        log.error(f"api_rekomendasi error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/data")
def api_data():
    """Endpoint utama — gabung semua data untuk dashboard."""
    kurs_data   = _fetch_kurs_live()
    harga_data  = _load_json("live_summary.json") or {}
    berita_data = _load_json("live_rss.json") or []
    spark_data  = _load_json("spark_results.json") or {}

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
    ts_files = list(DATA_DIR.glob("timeseries_*.json"))
    return jsonify({
        "status"      : "ok",
        "timestamp"   : datetime.now().isoformat(),
        "files"       : {f: (DATA_DIR / f).exists() for f in
                         ["live_summary.json", "live_rss.json", "spark_results.json"]},
        "timeseries"  : len(ts_files),
        "llm_cache"   : {
            "active"  : bool(_rek_cache["text"]),
            "sumber"  : _rek_cache.get("sumber"),
            "age_s"   : int(time.time() - _rek_cache["ts"]) if _rek_cache["ts"] else None,
        },
        "env"         : {
            "ANTHROPIC_API_KEY": bool(os.environ.get("ANTHROPIC_API_KEY")),
            "GEMINI_API_KEY"   : bool(os.environ.get("GEMINI_API_KEY")),
        },
    })


if __name__ == "__main__":
    log.info("=" * 55)
    log.info("  FLASK DASHBOARD — Pangan Big Data Analytics")
    log.info("  http://localhost:5000")
    log.info(f"  Claude key : {'✓ SET' if os.environ.get('ANTHROPIC_API_KEY') else '✗ kosong'}")
    log.info(f"  Gemini key : {'✓ SET' if os.environ.get('GEMINI_API_KEY') else '✗ kosong'}")
    log.info("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False)