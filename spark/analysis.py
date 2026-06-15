#!/usr/bin/env python3
"""
SPARK ANALYSIS — Kurs USD vs Harga Pangan Indonesia
====================================================
6 analisis:
  1. Volatilitas harga per komoditas
  2. Korelasi kurs USD/IDR vs harga pangan
  3. Matriks korelasi antar komoditas (→ Heatmap dashboard)
  4. Frekuensi komoditas di berita RSS
  5. Prediksi harga berbasis kurs (Spark MLlib LinearRegression)
  6. Simulasi dampak kebijakan subsidi [FITUR UNGGULAN]

Jalankan lokal:
  python spark/analysis.py

Jalankan via Spark cluster:
  spark-submit --master spark://localhost:7077 spark/analysis.py
"""

import json, os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

HDFS_BASE   = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020") + "/data/pangan"
LOCAL_INPUT = Path(os.getenv("LOCAL_INPUT_DIR", str(Path(__file__).parent.parent / "dashboard" / "data")))
OUTPUT_DIR  = Path(os.getenv("OUTPUT_DIR", str(Path(__file__).parent.parent / "dashboard" / "data")))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Bobot komoditas dalam pengeluaran keluarga miskin (BPS Susenas 2023)
BOBOT_BELANJA = {
    "Beras IR I": 18.5, "Beras IR III": 12.0,
    "Cabai Rawit Merah": 3.2, "Cabai Merah Keriting": 2.8,
    "Bawang Merah": 2.5, "Bawang Putih": 1.8,
    "Daging Sapi": 4.5, "Daging Ayam": 6.2,
    "Telur Ayam": 5.8, "Gula Pasir": 3.5, "Minyak Goreng": 4.8,
}

KONSUMSI_KG = {
    "Beras IR I": 10, "Beras IR III": 15, "Cabai Rawit Merah": 1.5,
    "Cabai Merah Keriting": 1.2, "Bawang Merah": 1.0, "Bawang Putih": 0.8,
    "Daging Sapi": 0.5, "Daging Ayam": 2.0, "Telur Ayam": 2.5,
    "Gula Pasir": 1.5, "Minyak Goreng": 2.0,
}

KELUARGA_MISKIN = 26_360_000 // 4   # BPS 2024


def init_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("PanganKursAnalysis")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())


# ════════════════════════════════════════════════════════
# LOAD DATA
# ════════════════════════════════════════════════════════
def load_data(spark: SparkSession):
    # Prioritas 1: HDFS
    try:
        df_api = spark.read.json(f"{HDFS_BASE}/pangan-api/*/*/*/*.json")
        df_rss = spark.read.json(f"{HDFS_BASE}/pangan-rss/*/*/*/*.json")
        print("✓ Data dari HDFS")
    except Exception:
        # Prioritas 2: file lokal dari consumer
        api_file = LOCAL_INPUT / "live_api.json"
        rss_file = LOCAL_INPUT / "live_rss.json"

        if api_file.exists():
            df_api = spark.read.json(str(api_file))
        else:
            raise FileNotFoundError("Tidak ada data. Jalankan producer + consumer dulu.")

        if rss_file.exists():
            df_rss = spark.read.json(str(rss_file))
        else:
            schema = StructType([
                StructField("judul", StringType()),
                StructField("relevan_pangan", BooleanType()),
                StructField("komoditas_tag", ArrayType(StringType())),
            ])
            df_rss = spark.createDataFrame([], schema=schema)
        print("✓ Data dari file lokal")

    # Sanitasi Null & Empty
    if df_api is not None and "komoditas" in df_api.columns:
        df_api = df_api.filter(F.col("komoditas").isNotNull() & (F.col("komoditas") != ""))
    if df_rss is not None and "judul" in df_rss.columns:
        df_rss = df_rss.filter(F.col("judul").isNotNull())

    return df_api, df_rss


# ════════════════════════════════════════════════════════
# ANALISIS 1: Volatilitas Harga
# ════════════════════════════════════════════════════════
def analisis_volatilitas(df_api) -> list:
    print("\n[1] Volatilitas Harga ...")
    hasil = (df_api
        .groupBy("komoditas")
        .agg(
            F.mean("harga_rp").alias("harga_rata2"),
            F.stddev("harga_rp").alias("harga_stddev"),
            F.max("harga_rp").alias("harga_max"),
            F.min("harga_rp").alias("harga_min"),
            F.count("harga_rp").alias("n"),
        )
        .withColumn("volatilitas_pct",
            F.round((F.col("harga_stddev") / F.col("harga_rata2")) * 100, 2))
        .withColumn("range_pct",
            F.round(((F.col("harga_max") - F.col("harga_min")) / F.col("harga_rata2")) * 100, 2))
        .orderBy(F.col("volatilitas_pct").desc())
    )
    return [r.asDict() for r in hasil.collect()]


# ════════════════════════════════════════════════════════
# ANALISIS 2: Korelasi Kurs vs Harga
# ════════════════════════════════════════════════════════
def analisis_korelasi_kurs(df_api) -> list:
    print("\n[2] Korelasi Kurs vs Harga ...")
    hasil = []
    for row in df_api.select("komoditas").distinct().collect():
        komoditas = row["komoditas"]
        if komoditas is None:
            continue
        df_k = df_api.filter(F.col("komoditas") == komoditas) \
                     .select("harga_rp", "kurs_usd_idr").dropna()
        if df_k.count() < 5:
            continue

        corr  = df_k.stat.corr("kurs_usd_idr", "harga_rp")
        stats = df_k.agg(
            F.mean("harga_rp").alias("harga_rata2"),
            F.mean("kurs_usd_idr").alias("kurs_rata2"),
            F.count("harga_rp").alias("n"),
        ).collect()[0]

        interp = (
            "Sangat kuat" if abs(corr) > 0.8 else
            "Kuat"        if abs(corr) > 0.6 else
            "Sedang"      if abs(corr) > 0.4 else
            "Lemah"       if abs(corr) > 0.2 else
            "Sangat lemah"
        )
        hasil.append({
            "komoditas"    : komoditas,
            "korelasi"     : round(corr, 4),
            "interpretasi" : interp,
            "harga_rata2"  : round(stats["harga_rata2"]),
            "kurs_rata2"   : round(stats["kurs_rata2"]),
            "n_data"       : stats["n"],
        })

    hasil.sort(key=lambda x: abs(x["korelasi"]), reverse=True)
    return hasil


# ════════════════════════════════════════════════════════
# ANALISIS 3: Heatmap Korelasi Antar Komoditas [UNGGULAN]
# ════════════════════════════════════════════════════════
def analisis_heatmap(df_api, spark) -> dict:
    print("\n[3] Heatmap Korelasi Antar Komoditas ...")
    komoditas_list = sorted(
        r["komoditas"] for r in df_api.select("komoditas").distinct().collect()
        if r["komoditas"] is not None
    )

    # Pivot: satu baris per timestamp, kolom = harga per komoditas (explicit list to exclude null columns)
    df_pivot = (df_api
        .groupBy("timestamp")
        .pivot("komoditas", komoditas_list)
        .agg(F.mean("harga_rp"))
        .dropna()
    )
    df_pivot.cache()

    if df_pivot.count() < 3:
        df_pivot.unpersist()
        return {"komoditas": komoditas_list, "matrix": [],
                "catatan": "Data belum cukup untuk heatmap"}

    matrix = []
    for k1 in komoditas_list:
        row = []
        for k2 in komoditas_list:
            if k1 == k2:
                row.append(1.0)
            elif k1 in df_pivot.columns and k2 in df_pivot.columns:
                corr = df_pivot.stat.corr(k1, k2)
                row.append(round(corr, 3) if corr is not None else 0.0)
            else:
                row.append(0.0)
        matrix.append(row)

    df_pivot.unpersist()
    return {"komoditas": komoditas_list, "matrix": matrix}


# ════════════════════════════════════════════════════════
# ANALISIS 4: Frekuensi Komoditas di Berita RSS
# ════════════════════════════════════════════════════════
def analisis_berita(df_rss) -> dict:
    print("\n[4] Teks Berita RSS ...")
    if df_rss is None or df_rss.count() == 0:
        return {"total_artikel": 0, "frekuensi": {}, "relevan_pangan": 0}
    try:
        total  = df_rss.count()
        rel_ct = df_rss.filter(F.col("relevan_pangan") == True).count()
        frek   = {}
        if "komoditas_tag" in df_rss.columns:
            df_tags = (df_rss
                .select(F.explode("komoditas_tag").alias("tag"))
                .groupBy("tag")
                .agg(F.count("*").alias("frekuensi"))
                .orderBy(F.col("frekuensi").desc())
            )
            frek = {r["tag"]: r["frekuensi"] for r in df_tags.collect()}
        return {"total_artikel": total, "relevan_pangan": rel_ct, "frekuensi": frek}
    except Exception as e:
        return {"total_artikel": 0, "error": str(e)}


# ════════════════════════════════════════════════════════
# ANALISIS 5: Prediksi Harga — Spark MLlib
# ════════════════════════════════════════════════════════
def analisis_prediksi(df_api, spark) -> list:
    print("\n[5] Prediksi Harga — MLlib LinearRegression ...")
    hasil          = []
    KURS_PREDIKSI  = [17_500, 18_000, 18_500, 19_000]
    assembler      = VectorAssembler(inputCols=["kurs_usd_idr"], outputCol="features")

    for row in df_api.select("komoditas").distinct().collect():
        komoditas = row["komoditas"]
        if komoditas is None:
            continue
        df_k = (df_api
            .filter(F.col("komoditas") == komoditas)
            .select("harga_rp", "kurs_usd_idr")
            .dropna()
        )
        if df_k.count() < 10:
            continue

        df_vec = assembler.transform(df_k).select("features", F.col("harga_rp").alias("label"))
        try:
            model = LinearRegression(maxIter=100, regParam=0.01).fit(df_vec)
            prediksi = {}
            for kurs in KURS_PREDIKSI:
                pred_df  = spark.createDataFrame([[kurs]], ["kurs_usd_idr"])
                pred_vec = assembler.transform(pred_df)
                pred     = model.transform(pred_vec).collect()[0]["prediction"]
                prediksi[str(kurs)] = round(pred)

            hasil.append({
                "komoditas"  : komoditas,
                "r2_score"   : round(model.summary.r2, 4),
                "rmse"       : round(model.summary.rootMeanSquaredError, 2),
                "koef_kurs"  : round(float(model.coefficients[0]), 4),
                "intercept"  : round(float(model.intercept), 2),
                "prediksi"   : prediksi,
            })
        except Exception as e:
            print(f"  [WARN] Prediksi {komoditas} gagal: {e}")

    return hasil


# ════════════════════════════════════════════════════════
# ANALISIS 6: Simulasi Kebijakan Subsidi [UNGGULAN]
# ════════════════════════════════════════════════════════
def analisis_simulasi_subsidi(hasil_prediksi: list, kurs_current: float = 18_100) -> dict:
    print("\n[6] Simulasi Kebijakan Subsidi ...")

    # Ambil harga prediksi untuk kurs Rp18.000
    harga_pred = {}
    for p in hasil_prediksi:
        pred = p.get("prediksi", {})
        harga_pred[p["komoditas"]] = pred.get("18000", pred.get("18500", 0))

    skenario = []
    for komoditas, harga in harga_pred.items():
        if harga <= 0:
            continue
        subsidi_pct = 0.10
        subsidi_rp  = round(harga * subsidi_pct)
        bobot       = BOBOT_BELANJA.get(komoditas, 3.0) / 100
        konsumsi    = KONSUMSI_KG.get(komoditas, 1)
        skenario.append({
            "komoditas"           : komoditas,
            "harga_sekarang"      : round(harga),
            "subsidi_rp_per_unit" : subsidi_rp,
            "harga_setelah"       : round(harga - subsidi_rp),
            "penurunan_pct"       : 10.0,
            "dampak_inflasi_ppt"  : -round(bobot * subsidi_pct * 100, 3),
            "anggaran_rp"         : round(subsidi_rp * konsumsi * KELUARGA_MISKIN),
            "keluarga_terbantu"   : KELUARGA_MISKIN,
        })

    return {
        "kurs_simulasi"            : kurs_current,
        "populasi_miskin"          : 26_360_000,
        "keluarga_miskin"          : KELUARGA_MISKIN,
        "skenario_subsidi_10pct"   : skenario,
        "total_dampak_inflasi_ppt" : round(sum(s["dampak_inflasi_ppt"] for s in skenario), 3),
        "total_anggaran_rp"        : sum(s["anggaran_rp"] for s in skenario),
        "catatan"                  : "Subsidi 10% per komoditas untuk 26,36 juta penduduk miskin (BPS 2024)",
    }


# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  SPARK ANALYSIS — Kurs USD vs Harga Pangan Indonesia")
    print("=" * 60)

    spark = init_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✓ Spark {spark.version}")

    df_api, df_rss = load_data(spark)
    df_api.cache()
    print(f"✓ Data API: {df_api.count():,} records | RSS: {df_rss.count():,} artikel")

    hasil_volatilitas = analisis_volatilitas(df_api)
    hasil_korelasi    = analisis_korelasi_kurs(df_api)
    hasil_heatmap     = analisis_heatmap(df_api, spark)
    hasil_berita      = analisis_berita(df_rss)
    hasil_prediksi    = analisis_prediksi(df_api, spark)
    hasil_subsidi     = analisis_simulasi_subsidi(hasil_prediksi)

    output = {
        "generated_at"    : datetime.now().isoformat(),
        "volatilitas"     : hasil_volatilitas,
        "korelasi_kurs"   : hasil_korelasi,
        "heatmap"         : hasil_heatmap,
        "berita"          : hasil_berita,
        "prediksi"        : hasil_prediksi,
        "simulasi_subsidi": hasil_subsidi,
    }

    out_path = OUTPUT_DIR / "spark_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Hasil → {out_path.resolve()}")
    print(f"\n{'='*60}")
    print(f"  Volatilitas : {len(hasil_volatilitas)} komoditas")
    print(f"  Korelasi    : {len(hasil_korelasi)} komoditas")
    print(f"  Heatmap     : {len(hasil_heatmap.get('komoditas', []))}×{len(hasil_heatmap.get('komoditas', []))}")
    print(f"  Berita      : {hasil_berita.get('total_artikel', 0)} artikel")
    print(f"  Prediksi    : {len(hasil_prediksi)} model")
    print(f"  Subsidi     : {len(hasil_subsidi.get('skenario_subsidi_10pct', []))} skenario")
    print("="*60)

    spark.stop()


if __name__ == "__main__":
    main()
