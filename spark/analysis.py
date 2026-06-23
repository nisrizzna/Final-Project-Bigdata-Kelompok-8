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
from pyspark.sql import Window
from pyspark.sql import Window
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.sql import Window
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.evaluation import RegressionEvaluator

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
# HELPER: Sentiment per Komoditas dari live_rss.json
# ════════════════════════════════════════════════════════
def load_sentiment_per_komoditas(spark):
    rss_file = LOCAL_INPUT / "live_rss.json"
    if not rss_file.exists():
        print("  [WARN] live_rss.json tidak ditemukan, sentiment default 0")
        return {}
    try:
        df_rss = spark.read.json(str(rss_file))
        if "komoditas_tag" not in df_rss.columns or "sentiment_score" not in df_rss.columns:
            return {}
        df_expl = (df_rss
            .filter(F.col("relevan_pangan") == True)
            .select(F.explode("komoditas_tag").alias("komoditas"), "sentiment_score")
            .dropna()
        )
        result = (df_expl
            .groupBy("komoditas")
            .agg(F.mean("sentiment_score").alias("sentiment_avg"))
            .collect()
        )
        return {r["komoditas"]: round(float(r["sentiment_avg"]), 3) for r in result}
    except Exception as e:
        print(f"  [WARN] Gagal load sentiment: {e}")
        return {}


# ════════════════════════════════════════════════════════
# HELPER: Sentiment per Komoditas dari live_rss.json
# ════════════════════════════════════════════════════════
def load_sentiment_per_komoditas(spark):
    rss_file = LOCAL_INPUT / "live_rss.json"
    if not rss_file.exists():
        print("  [WARN] live_rss.json tidak ditemukan, sentiment default 0")
        return {}
    try:
        df_rss = spark.read.json(str(rss_file))
        if "komoditas_tag" not in df_rss.columns or "sentiment_score" not in df_rss.columns:
            return {}
        df_expl = (df_rss
            .filter(F.col("relevan_pangan") == True)
            .select(F.explode("komoditas_tag").alias("komoditas"), "sentiment_score")
            .dropna()
        )
        result = (df_expl
            .groupBy("komoditas")
            .agg(F.mean("sentiment_score").alias("sentiment_avg"))
            .collect()
        )
        return {r["komoditas"]: round(float(r["sentiment_avg"]), 3) for r in result}
    except Exception as e:
        print(f"  [WARN] Gagal load sentiment: {e}")
        return {}


# ════════════════════════════════════════════════════════
# ANALISIS 5: Prediksi Harga — GBTRegressor Multi-Fitur
# ════════════════════════════════════════════════════════
def analisis_prediksi(df_api, spark):
    print("\n[5] Prediksi Harga — GBTRegressor Multi-Fitur ...")
    ts_files = sorted(LOCAL_INPUT.glob("timeseries_*.json"))
    if not ts_files:
        print("  [WARN] Tidak ada timeseries_*.json — fallback ke live_api.json")
        return _analisis_prediksi_fallback(df_api, spark)

    sentiment_map = load_sentiment_per_komoditas(spark)
    print(f"  ✓ Sentiment: {len(sentiment_map)} komoditas")
    hasil = []

    for ts_file in ts_files:
        try:
            df_ts = spark.read.json(str(ts_file))
        except Exception as e:
            print(f"  [SKIP] {ts_file.name}: gagal load — {e}")
            continue

        n_total = df_ts.count()
        if n_total < 3:
            print(f"  [SKIP] {ts_file.name}: hanya {n_total} baris (minimal 3)")
            continue

        komoditas_val = df_ts.select("komoditas").first()["komoditas"]
        if not komoditas_val:
            continue

        df_ts = (df_ts
            .withColumn("date",             F.to_date("tanggal", "yyyy-MM-dd"))
            .withColumn("harga_avg",        F.col("harga_avg").cast("double"))
            .withColumn("kurs_avg",         F.col("kurs_avg").cast("double"))
            .withColumn("flag_lebaran",     F.col("flag_lebaran").cast("double"))
            .withColumn("flag_panen",       F.col("flag_panen").cast("double"))
            .withColumn("flag_impor_ekspor",F.col("flag_impor_ekspor").cast("double"))
            .orderBy("date")
        )
        w = Window.orderBy("date")
        df_feat = (df_ts
            .withColumn("lag_1",           F.lag("harga_avg", 1).over(w))
            .withColumn("lag_7",           F.lag("harga_avg", 7).over(w))
            .withColumn("kurs_7d_avg",     F.avg("kurs_avg").over(w.rowsBetween(-6, 0)))
            .withColumn("sentiment_score", F.lit(float(sentiment_map.get(komoditas_val, 0.0))))
        )

        has_lag7     = n_total >= 8
        feature_cols = ["kurs_avg","kurs_7d_avg","flag_lebaran","flag_panen","flag_impor_ekspor","lag_1","sentiment_score"]
        if has_lag7:
            feature_cols.append("lag_7")

        df_feat = df_feat.dropna(subset=feature_cols + ["harga_avg"])
        n_rows  = df_feat.count()
        if n_rows < 3:
            print(f"  [SKIP] {komoditas_val}: setelah drop null hanya {n_rows} baris")
            continue

        w_rn      = Window.orderBy("date")
        df_num    = df_feat.withColumn("_rn", F.row_number().over(w_rn))
        cutoff    = max(int(n_rows * 0.8), n_rows - 3)
        df_tr_raw = df_num.filter(F.col("_rn") <= cutoff).drop("_rn")
        df_te_raw = df_num.filter(F.col("_rn") >  cutoff).drop("_rn")

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
        df_train  = assembler.transform(df_tr_raw).withColumnRenamed("harga_avg", "label")
        df_test   = assembler.transform(df_te_raw).withColumnRenamed("harga_avg", "label")

        try:
            model      = GBTRegressor(maxIter=20, maxDepth=3, stepSize=0.1, featuresCol="features", labelCol="label").fit(df_train)
            model_name = "GBTRegressor"
        except Exception as e:
            try:
                model      = LinearRegression(maxIter=100, regParam=0.01, featuresCol="features", labelCol="label").fit(df_train)
                model_name = "LinearRegression"
            except Exception as e2:
                print(f"  [ERROR] {komoditas_val}: {e2}")
                continue

        mae, mape_pct = 0.0, 0.0
        if df_test.count() > 0:
            try:
                preds    = model.transform(df_test)
                mae      = float(preds.withColumn("e", F.abs(F.col("prediction")-F.col("label"))).agg(F.mean("e")).collect()[0][0] or 0.0)
                mape_pct = float(preds.withColumn("p", F.when(F.col("label")>0, F.abs((F.col("prediction")-F.col("label"))/F.col("label"))*100).otherwise(0.0)).agg(F.mean("p")).collect()[0][0] or 0.0)
            except Exception:
                pass

        latest      = df_feat.orderBy(F.col("date").desc()).limit(1).collect()[0]
        ld          = latest.asDict()
        latest_harga = float(ld.get("harga_avg") or 0)
        latest_kurs  = float(ld.get("kurs_avg")  or 0)
        latest_kurs7 = float(ld.get("kurs_7d_avg") or latest_kurs)
        latest_lag1  = float(ld.get("lag_1")  or latest_harga)
        latest_lag7  = float(ld.get("lag_7")  or latest_harga) if has_lag7 else latest_harga
        latest_leb   = float(ld.get("flag_lebaran")      or 0)
        latest_panen = float(ld.get("flag_panen")        or 0)
        latest_impor = float(ld.get("flag_impor_ekspor") or 0)
        latest_sent  = float(sentiment_map.get(komoditas_val, 0.0))

        kurs_proj = {7: latest_kurs*1.005, 14: latest_kurs*1.01, 30: latest_kurs*1.02}
        prediksi  = {}
        for hari, kurs_p in kurs_proj.items():
            rd = {"kurs_avg":kurs_p,"kurs_7d_avg":latest_kurs7,"flag_lebaran":latest_leb,"flag_panen":latest_panen,"flag_impor_ekspor":latest_impor,"lag_1":latest_lag1,"sentiment_score":latest_sent}
            if has_lag7: rd["lag_7"] = latest_lag7
            pv = float(model.transform(assembler.transform(spark.createDataFrame([rd]))).collect()[0]["prediction"])
            prediksi[hari] = round(pv)

        stats       = df_ts.agg(F.mean("harga_avg").alias("mean"), F.stddev("harga_avg").alias("std")).collect()[0]
        h_mean      = float(stats["mean"] or latest_harga)
        h_std       = float(stats["std"]  or latest_harga*0.05)
        thr_waspada = round(h_mean + 1.5*h_std)
        thr_bahaya  = round(h_mean + 2.0*h_std)

        alerts = {}
        for hari, pv in prediksi.items():
            status = "BAHAYA" if pv>=thr_bahaya else "WASPADA" if pv>=thr_waspada else "AMAN"
            alerts[f"t{hari}"] = {"hari_ke":hari,"prediksi":pv,"threshold_waspada":thr_waspada,"threshold_bahaya":thr_bahaya,"status":status}

        print(f"  ✓ {komoditas_val:25s} | {model_name:16s} | t+7: Rp{prediksi[7]:>9,.0f} | t+14: Rp{prediksi[14]:>9,.0f} | t+30: Rp{prediksi[30]:>9,.0f} | MAE: {mae:>8,.0f} | MAPE: {mape_pct:.1f}%")
        hasil.append({"komoditas":komoditas_val,"model":model_name,"n_data":n_rows,"harga_saat_ini":round(latest_harga),"kurs_saat_ini":round(latest_kurs),"prediksi_7h":prediksi[7],"prediksi_14h":prediksi[14],"prediksi_30h":prediksi[30],"mae":round(mae,2),"mape_pct":round(mape_pct,2),"alerts":alerts,"prediksi":{"18000":prediksi[14],"18500":prediksi[30]}})

    print(f"\n  ✓ Total: {len(hasil)} komoditas diproses")
    if not hasil:
        print("  [INFO] Semua timeseries < 3 baris — fallback ke live_api.json")
        return _analisis_prediksi_fallback(df_api, spark)
    return hasil


def _analisis_prediksi_fallback(df_api, spark):
    print("  [FALLBACK] Multi-fitur dari live_api.json ...")
    feature_cols = ["kurs_usd_idr","kurs_7d_avg","flag_lebaran","flag_panen","flag_impor_ekspor","harga_kemarin"]
    hasil = []
    komoditas_list = [r["komoditas"] for r in df_api.select("komoditas").distinct().collect() if r["komoditas"]]

    for komoditas in komoditas_list:
        df_k = df_api.filter(F.col("komoditas")==komoditas).select(["harga_rp"]+feature_cols).dropna()
        n_rows = df_k.count()
        if n_rows < 3:
            continue
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
        df_vec    = assembler.transform(df_k).withColumnRenamed("harga_rp", "label")
        w_rn      = Window.orderBy(F.monotonically_increasing_id())
        df_num    = df_vec.withColumn("_rn", F.row_number().over(w_rn))
        cutoff    = max(int(n_rows*0.8), n_rows-2)
        df_train  = df_num.filter(F.col("_rn")<=cutoff).drop("_rn")
        df_test   = df_num.filter(F.col("_rn")>cutoff).drop("_rn")

        try:
            model      = GBTRegressor(maxIter=20, maxDepth=3, stepSize=0.1, featuresCol="features", labelCol="label").fit(df_train)
            model_name = "GBT(fallback)"
        except Exception:
            try:
                model      = LinearRegression(maxIter=100, regParam=0.01, featuresCol="features", labelCol="label").fit(df_train)
                model_name = "LR(fallback)"
            except Exception as e2:
                print(f"  [ERROR] {komoditas}: {e2}")
                continue

        mae, mape_pct = 0.0, 0.0
        if df_test.count() > 0:
            try:
                preds    = model.transform(df_test)
                mae      = float(preds.withColumn("e", F.abs(F.col("prediction")-F.col("label"))).agg(F.mean("e")).collect()[0][0] or 0.0)
                mape_pct = float(preds.withColumn("p", F.when(F.col("label")>0, F.abs((F.col("prediction")-F.col("label"))/F.col("label"))*100).otherwise(0.0)).agg(F.mean("p")).collect()[0][0] or 0.0)
            except Exception:
                pass

        latest   = df_k.orderBy(F.monotonically_increasing_id().desc()).limit(1).collect()[0]
        ld       = latest.asDict()
        lh       = float(ld.get("harga_rp")          or 0)
        lk       = float(ld.get("kurs_usd_idr")      or 0)
        lk7      = float(ld.get("kurs_7d_avg")       or lk)
        lkem     = float(ld.get("harga_kemarin")      or lh)
        lleb     = float(ld.get("flag_lebaran")       or 0)
        lpanen   = float(ld.get("flag_panen")         or 0)
        limpor   = float(ld.get("flag_impor_ekspor")  or 0)

        kurs_proj = {7:lk*1.005, 14:lk*1.01, 30:lk*1.02}
        prediksi  = {}
        for hari, kurs_p in kurs_proj.items():
            rd = {"kurs_usd_idr":kurs_p,"kurs_7d_avg":lk7,"flag_lebaran":lleb,"flag_panen":lpanen,"flag_impor_ekspor":limpor,"harga_kemarin":lkem}
            pv = float(model.transform(assembler.transform(spark.createDataFrame([rd]))).collect()[0]["prediction"])
            prediksi[hari] = round(pv)

        stats       = df_k.agg(F.mean("harga_rp").alias("mean"), F.stddev("harga_rp").alias("std")).collect()[0]
        h_mean      = float(stats["mean"] or lh)
        h_std       = float(stats["std"]  or lh*0.05)
        thr_waspada = round(h_mean + 1.5*h_std)
        thr_bahaya  = round(h_mean + 2.0*h_std)

        alerts = {}
        for hari, pv in prediksi.items():
            status = "BAHAYA" if pv>=thr_bahaya else "WASPADA" if pv>=thr_waspada else "AMAN"
            alerts[f"t{hari}"] = {"hari_ke":hari,"prediksi":pv,"threshold_waspada":thr_waspada,"threshold_bahaya":thr_bahaya,"status":status}

        print(f"  ✓ {komoditas:25s} | {model_name:12s} | t+7: Rp{prediksi[7]:>9,.0f} | t+14: Rp{prediksi[14]:>9,.0f} | t+30: Rp{prediksi[30]:>9,.0f} | MAE: {mae:>8,.0f} | MAPE: {mape_pct:.1f}%")
        hasil.append({"komoditas":komoditas,"model":model_name,"n_data":n_rows,"harga_saat_ini":round(lh),"kurs_saat_ini":round(lk),"prediksi_7h":prediksi[7],"prediksi_14h":prediksi[14],"prediksi_30h":prediksi[30],"mae":round(mae,2),"mape_pct":round(mape_pct,2),"alerts":alerts,"prediksi":{"18000":prediksi[14],"18500":prediksi[30]}})

    print(f"\n  ✓ Fallback selesai: {len(hasil)} komoditas")
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
        harga_pred[p["komoditas"]] = p.get("prediksi_14h") or pred.get("18000", pred.get("18500", 0))

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

    # Kumpulkan alerts aktif
    semua_alerts = []
    for p in hasil_prediksi:
        for horizon, alert in p.get("alerts", {}).items():
            if alert.get("status") in ("WASPADA", "BAHAYA"):
                semua_alerts.append({"komoditas":p["komoditas"],"horizon":horizon,"hari_ke":alert["hari_ke"],"prediksi":alert["prediksi"],"threshold_waspada":alert["threshold_waspada"],"threshold_bahaya":alert["threshold_bahaya"],"status":alert["status"],"harga_saat_ini":p.get("harga_saat_ini",0)})
    semua_alerts.sort(key=lambda x: x["status"], reverse=True)

    n_model = max(len(hasil_prediksi), 1)
    evaluasi_model = {"n_komoditas_diproses":len(hasil_prediksi),"rata_rata_mae":round(sum(p.get("mae",0) for p in hasil_prediksi)/n_model,2),"rata_rata_mape_pct":round(sum(p.get("mape_pct",0) for p in hasil_prediksi)/n_model,2),"detail_per_komoditas":[{"komoditas":p["komoditas"],"model":p.get("model",""),"n_data":p.get("n_data",0),"mae":p.get("mae",0),"mape_pct":p.get("mape_pct",0)} for p in hasil_prediksi]}

    output = {
        "generated_at"    : datetime.now().isoformat(),
        "volatilitas"     : hasil_volatilitas,
        "korelasi_kurs"   : hasil_korelasi,
        "heatmap"         : hasil_heatmap,
        "berita"          : hasil_berita,
        "prediksi"        : hasil_prediksi,
        "alerts"          : semua_alerts,
        "evaluasi_model"  : evaluasi_model,
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
