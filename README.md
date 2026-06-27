# Big Data Analytics — Prediksi & Stabilisasi Harga Pangan Indonesia

**Kelompok 8** · Semester 4 · Institut Teknologi Sepuluh Nopember (ITS)  
Stack: Apache Kafka · Hadoop HDFS · Apache Spark GBT MLlib · Flask · LLM (Claude/Gemini)

---

<<<<<<< HEAD
## Daftar Anggota
=======
Kurs dolar AS menembus Rp18.209 pada 9 Juni 2026. Indonesia masih mengimpor gandum 10,6 juta ton/tahun, kedelai 2,5 juta ton/tahun, gula 4,4 juta ton/tahun. Sistem ini memprediksi harga 11 komoditas pangan strategis untuk 7, 14, dan 30 hari ke depan, dilengkapi deteksi penyebab kenaikan harga, peringatan dini otomatis, dan rekomendasi intervensi berbasis LLM (Claude/Gemini).
>>>>>>> fab2d29 (fix)

| No | Nama | NRP |
|----|------|-----|
| 1  | Revalina Erica P. | 5027241007 |
| 2  | Syifa Nurul A. | 5027241019 |
| 3  | Azaria Raissa M. | 5027241043 |
| 4  | Nisrina Bilqis | 5027241054 |

---

## Latar Belakang Masalah

Kurs dolar AS menembus **Rp18.209 pada 9 Juni 2026** — rekor tertinggi sepanjang sejarah. Indonesia masih bergantung pada impor komoditas strategis:

| Komoditas | Volume Impor/Tahun |
|-----------|-------------------|
| Gandum    | 10,6 juta ton |
| Kedelai   | 2,5 juta ton |
| Gula      | 4,4 juta ton |

Kenaikan kurs berdampak langsung pada harga pangan di tingkat konsumen. Sistem peringatan dini berbasis data belum banyak tersedia di Indonesia yang bisa memprediksi lonjakan harga sebelum terjadi.

**Masalah:** Pemerintah dan pelaku pasar tidak memiliki sistem terukur yang bisa memprediksi harga 7–30 hari ke depan dengan mempertimbangkan faktor kurs, sentimen berita, dan musiman secara bersamaan.

**Solusi kami:** Platform Big Data end-to-end yang memproses data streaming real-time, melatih model prediktif GBT per komoditas, dan menghasilkan rekomendasi intervensi kebijakan berbasis LLM secara otomatis.

---

## Deskripsi Masalah

Ketidakstabilan harga pangan adalah ancaman nyata bagi 270 juta penduduk Indonesia, khususnya 26,36 juta keluarga miskin yang mengalokasikan lebih dari 60% pengeluaran untuk pangan (BPS, 2024). Lonjakan kurs USD/IDR ke **Rp18.209 (9 Juni 2026)** — tertinggi sepanjang sejarah — secara langsung menaikkan biaya impor gandum, kedelai, dan gula yang menopang rantai pangan nasional.

Permasalahan utama: **tidak ada sistem peringatan dini berbasis data yang dapat memprediksi kenaikan harga 7–30 hari ke depan** sebelum berdampak ke konsumen. Sistem yang ada hanya memantau harga historis, bukan memproyeksikan ke depan. Akibatnya, intervensi kebijakan (operasi pasar, impor darurat) selalu terlambat dan reaktif.

## Tujuan Proyek

- Membangun pipeline data streaming **real-time** menggunakan Apache Kafka untuk mengalirkan data harga pangan, kurs USD/IDR, dan sentimen berita ekonomi secara bersamaan
- Menyimpan dan mengelola data time series per komoditas di **Hadoop HDFS** dengan partisi by tanggal untuk mendukung analisis historis
- Melatih model **Gradient Boosted Trees (GBTRegressor)** via Apache Spark MLlib untuk memprediksi harga 7, 14, dan 30 hari ke depan dengan 8 fitur multi-dimensi
- Membangun sistem **peringatan dini otomatis** berbasis threshold statistik (mean ± 1.5σ / 2.0σ) per komoditas
- Mengintegrasikan **LLM (Claude/Gemini)** untuk menghasilkan rekomendasi intervensi kebijakan yang actionable berdasarkan alert aktif dan berita relevan
- Menyajikan seluruh analisis dalam **dashboard real-time** yang dapat diakses langsung oleh pengambil keputusan

---

## Diagram Alur Sistem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          SUMBER DATA (Real-Time)                        │
│                                                                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │
│  │  hargapangan.id │  │  yfinance API    │  │  RSS Berita Ekonomi  │   │
│  │  (7 komoditas)  │  │  (Kurs USD/IDR)  │  │  (Kontan, Kompas...) │   │
│  └────────┬────────┘  └────────┬─────────┘  └──────────┬───────────┘   │
└───────────┼────────────────────┼───────────────────────┼───────────────┘
            │                    │                        │
            ▼                    ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LAYER 1 — KAFKA STREAMING                           │
│                                                                         │
│  ┌───────────────────────┐        ┌──────────────────────────────────┐  │
│  │  producer_api.py      │        │  producer_rss.py                 │  │
│  │  • Harga + kurs live  │        │  • Crawl berita 5 sumber         │  │
│  │  • Fitur turunan:     │        │  • Sentiment scoring (+1/0/-1)   │  │
│  │    - lag_1 (harga-1)  │        │  • Ekstrak penyebab_dominan:     │  │
│  │    - kurs_7d_avg      │        │    kurs/cuaca/kebijakan/pasokan  │  │
│  │    - flag_lebaran      │        │  • Tag komoditas per artikel     │  │
│  │    - flag_panen        │        │                                  │  │
│  │    - flag_impor_ekspor │        │  Topic: pangan_berita            │  │
│  │  Topic: pangan_data    │        └──────────────────────────────────┘  │
│  └───────────────────────┘                                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LAYER 2 — CONSUMER & HDFS STORAGE                  │
│                                                                         │
│  consumer_to_hdfs.py                                                    │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Kafka Consumer (auto-reconnect, earliest offset)                │   │
│  │         │                                                        │   │
│  │         ├──► HDFS: /data/pangan/timeseries/{komoditas}/{tanggal} │   │
│  │         │           (partisi per komoditas per hari)              │   │
│  │         │                                                        │   │
│  │         └──► Local: dashboard/data/                              │   │
│  │                  ├── live_summary.json   (harga live)             │   │
│  │                  ├── live_api.json       (raw harga+fitur)        │   │
│  │                  ├── live_rss.json       (berita+sentimen)        │   │
│  │                  └── timeseries_{slug}.json  (array 30 hari)     │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LAYER 3 — SPARK ML ANALYTICS                        │
│                                                                         │
│  spark/analysis.py  (dijalankan tiap 2 menit via scheduler)            │
│                                                                         │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │  Analisis 1  │  │  Analisis 2      │  │  Analisis 3              │  │
│  │  Korelasi    │  │  Volatilitas     │  │  Heatmap Korelasi        │  │
│  │  Kurs vs     │  │  Harga (CoV)     │  │  Antar Komoditas         │  │
│  │  Harga       │  │                  │  │  (Spark stat.corr pivot) │  │
│  └──────────────┘  └──────────────────┘  └──────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Analisis 5 — MODEL PREDIKSI UTAMA (GBTRegressor)               │   │
│  │                                                                  │   │
│  │  Input Fitur (VectorAssembler):                                  │   │
│  │  [kurs_avg, kurs_7d_avg, flag_lebaran, flag_panen,              │   │
│  │   flag_impor_ekspor, lag_1, sentiment_score, lag_7*]            │   │
│  │                                                                  │   │
│  │  Model: GBTRegressor (maxIter=20, maxDepth=3, stepSize=0.1)     │   │
│  │  Fallback: LinearRegression (maxIter=100, regParam=0.01)         │   │
│  │                                                                  │   │
│  │  Output per komoditas:                                           │   │
│  │  ├── prediksi_7h, prediksi_14h, prediksi_30h                   │   │
│  │  ├── MAE + MAPE (evaluasi akurasi)                              │   │
│  │  └── alerts: AMAN / WASPADA / BAHAYA                            │   │
│  │      Threshold: WASPADA = mean + 1.5σ, BAHAYA = mean + 2.0σ    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Analisis 6 — Simulasi Kebijakan Subsidi 10%                    │   │
│  │  Estimasi dampak finansial terhadap 26,36 jt keluarga miskin    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Output: dashboard/data/spark_results.json                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LAYER 4 — FLASK DASHBOARD + LLM                    │
│                                                                         │
│  dashboard/app.py                                                       │
│                                                                         │
│  Endpoint:                                                              │
│  ├── GET /            → Halaman dashboard utama                         │
│  ├── GET /api/data    → Semua data gabungan (kurs+harga+berita+spark)  │
│  ├── GET /api/spark   → Hasil prediksi + alert Spark                   │
│  ├── GET /api/timeseries/<slug> → Data historis 30 hari per komoditas  │
│  └── GET /api/rekomendasi → [FITUR UNGGULAN] Rekomendasi LLM           │
│           Chain: Claude API → Gemini API → Rule-Based Fallback         │
│           Cache: 30 menit (tidak spam API)                              │
│                                                                         │
│  dashboard/templates/index.html                                         │
│  ├── 🛒 Harga pangan live (real-time)                                   │
│  ├── 🤖 Prediksi card per komoditas (7h/14h/30h + badge status)         │
│  ├── 🚨 Alert panel AMAN/WASPADA/BAHAYA                                 │
│  ├── 📈 Line chart historis 30 hari + garis prediksi ke depan          │
│  ├── 🧠 Panel rekomendasi LLM + tombol refresh                          │
│  ├── 📊 Chart korelasi kurs & volatilitas                               │
│  ├── 🔥 Heatmap korelasi antar komoditas                                │
│  └── ⚖️ Simulasi dampak subsidi kebijakan                               │
└─────────────────────────────────────────────────────────────────────────┘

Akses: http://localhost:5002
```

---

## Tech Stack

| Layer | Teknologi | Fungsi |
|-------|-----------|--------|
| Containerization | Docker, Docker Compose | Orchestrasi semua service |
| Data Streaming | Apache Kafka (KRaft) | Message broker real-time |
| Storage | Hadoop HDFS | Distributed storage time series |
| ML Engine | Apache Spark 3.5 + MLlib | GBT model training & inference |
| Feature Store | HDFS + local JSON | Fitur historis, musiman, sentimen |
| Backend API | Flask + Flask-CORS | REST API dashboard |
| LLM Integration | Claude API + Gemini API | Rekomendasi kebijakan otomatis |
| Frontend | HTML/CSS/JS + Chart.js | Dashboard interaktif real-time |
| Kurs Data | yfinance (USDIDR=X) | Kurs USD/IDR live |
| Berita | RSS Parsing | Sentimen & penyebab dominan |

---

## Struktur Direktori

```
<<<<<<< HEAD
fp-bigdata-kelompok8/
├── docker-compose.yml          ← Semua services (Kafka, HDFS, Spark, Flask)
├── hadoop.env                  ← Konfigurasi Hadoop
├── requirements.txt            ← Python dependencies
├── run.sh                      ← Script otomatis Linux/Mac
├── scripts/
│   └── init_infrastructure.py ← Init Kafka topics + HDFS dirs
├── kafka/
│   ├── producer_api.py         ← Producer harga pangan + kurs + fitur musiman
│   ├── producer_rss.py         ← Producer berita + sentiment scoring
│   └── consumer_to_hdfs.py    ← Consumer → HDFS + local JSON timeseries
├── spark/
│   └── analysis.py             ← 6 analisis Spark: korelasi, volatilitas,
│                                  heatmap, berita, GBT prediksi, subsidi
└── dashboard/
    ├── app.py                  ← Flask backend + endpoint /api/rekomendasi
    ├── Dockerfile
    ├── requirements-flask.txt
    ├── templates/
    │   └── index.html          ← Dashboard real-time (prediksi + alert + LLM)
    └── data/                   ← Auto-generated saat pipeline berjalan
        ├── live_summary.json
        ├── live_api.json
        ├── live_rss.json
        ├── spark_results.json
        └── timeseries_*.json   ← Per komoditas (slug)
=======
fp/
+-- docker-compose.yml        <- Semua services Docker
+-- hadoop.env                <- Konfigurasi Hadoop
+-- requirements.txt          <- Python dependencies
+-- run.sh                    <- Script otomatis Linux/Mac
+-- README.md                 <- File ini
+-- scripts/
|   +-- init_infrastructure.py  <- Init Kafka topics + HDFS
+-- kafka/
|   +-- producer_api.py      <- Producer kurs + harga pangan
|   +-- producer_rss.py      <- Producer berita RSS
|   +-- consumer_to_hdfs.py  <- Consumer ke HDFS + local
+-- spark/
|   +-- analysis.py          <- 6 analisis Spark termasuk GBTRegressor & Evaluasi Model
+-- dashboard/
    +-- app.py               <- Flask backend
    +-- Dockerfile
    +-- requirements-flask.txt
    +-- templates/index.html <- Dashboard real-time
    +-- data/                <- Auto-generated (spark_results.json memuat matriks ML & Alerts)

```

## Docker Services

| Komponen | Image / Source | Port | Fungsi |
| --- | --- | --- | --- |
| Kafka (KRaft) | confluentinc/cp-kafka:7.6.1 | 9092 | Message broker streaming |
| HDFS NameNode | bde2020/hadoop-namenode | 19870, 8020 | Storage HDFS Utama (UI: http://localhost:19870) |
| HDFS DataNode | bde2020/hadoop-datanode | - | Node penyimpanan data |
| YARN | bde2020/hadoop-resourcemanager | 8088 | Resource manager cluster |
| Spark Master | apache/spark:3.5.0 | 8080, 7077 | Spark Master Node (Scheduler internal running) |
| Spark Worker | apache/spark:3.5.0 | - | Spark Worker Node |
| Flask Dashboard | fp-flask (Dockerfile) | 5002 (Host) | Dashboard UI Utama (UI: http://localhost:5002) |
| Kafka Consumer | fp-consumer (Dockerfile.consumer) | - | Otomatis flush data ke HDFS & file lokal |

## Data Pipeline, Feature Engineering & ML Analytics

Komponen pipeline data telah diperluas untuk menyediakan fitur historis, sinyal musiman, dan pemodelan prediktif tingkat lanjut.

### 1. Ingesti Data & Fitur Musiman (`kafka/producer_api.py`)

* Kurs USD/IDR historis 35 hari dari `yfinance` diambil saat startup dengan mekanisme *backup simulation* jika API *offline*.
* Fitur turunan model prediktif dibuat secara dinamis: `harga_kemarin` (lag_1), `kurs_7d_avg`, dan `kurs_30d_avg`.
* Penyuntikan flag musiman kalender Indonesia: `flag_lebaran`, `flag_panen`, dan `flag_impor_ekspor`.

### 2. Analisis Sentimen Berita Ekonomi (`kafka/producer_rss.py`)

* *Scoring* sentimen otomatis berbasis *keyword matching* (+1 positif, 0 netral, -1 negatif) dari portal berita *real-time*.
* Ekstraksi `penyebab_dominan` ("kurs", "cuaca", "kebijakan", "pasokan", "umum") untuk kontekstualisasi grafik.

### 3. Pemodelan Prediktif & Evaluasi ML (`spark/analysis.py`)

* **Pipeline Multi-Fitur Advanced (Analisis 5):** Menggunakan **GBTRegressor (Gradient Boosted Trees)** sebagai model utama untuk memproyeksikan harga komoditas pada horizon **t+7, t+14, dan t+30 hari**.
* **Fitur Pemodelan:** Memanfaatkan `kurs_avg`, `kurs_7d_avg`, `flag_lebaran`, `flag_panen`, `flag_impor_ekspor`, `lag_1`, `sentiment_score` (dari live RSS), dan `lag_7` (jika data historis mencukupi).
* **Mekanisme Robust Fallback:** Jika data run-time per komoditas mengalami anomali atau gagal melatih pohon keputusan GBT, pipeline otomatis turun (*fallback*) ke **LinearRegression** agar visualisasi dashboard tidak terputus.
* **Evaluasi Performa Komprehensif:** Setiap iterasi model menghitung metrik **MAE (Mean Absolute Error)** dan **MAPE (Mean Absolute Percentage Error)** secara *real-time* untuk memantau tingkat akurasi prediksi langsung pada struktur data utama.

## Web UI Monitoring & Struktur Output JSON

| Service | URL |
| --- | --- |
| Dashboard Utama | http://localhost:5002 |
| HDFS NameNode | http://localhost:19870 |
| Spark Master | http://localhost:8080 |
| YARN | http://localhost:8088 |

File output utama `/dashboard/data/spark_results.json` distrukturkan dengan skema berikut untuk konsumsi *frontend*:

```json
{
  "generated_at": "ISO-Timestamp",
  "volatilitas": [...],
  "korelasi_kurs": [...],
  "heatmap": [...],
  "berita": [...],
  "prediksi": [
    {
      "komoditas": "Daging Sapi",
      "model": "GBT(fallback)",
      "prediksi_7h": 157062,
      "prediksi_14h": 157062,
      "prediksi_30h": 157062,
      "mae": 767.41,
      "mape_pct": 0.49,
      "alerts": { ... }
    }
  ],
  "alerts": [
    {
      "komoditas": "Cabai Rawit Merah",
      "horizon": "t7",
      "hari_ke": 7,
      "prediksi": 151793,
      "threshold_waspada": 120000,
      "threshold_bahaya": 140000,
      "status": "BAHAYA",
      "harga_saat_ini": 110000
    }
  ],
  "evaluasi_model": {
    "n_komoditas_diproses": 11,
    "rata_rata_mae": 7824.12,
    "rata_rata_mape_pct": 11.09,
    "detail_per_komoditas": [...]
  },
  "simulasi_subsidi": [...]
}

```

## Fitur Unggulan (Pembeda Utama dari KursAlert)

### 1. Heatmap Korelasi Antar Komoditas

Matriks korelasi harga semua komoditas menggunakan Spark pivot + `stat.corr` untuk melihat keterkaitan pergerakan antar pangan secara makro.

### 2. Simulasi Dampak Kebijakan Subsidi

Menghitung dampak finansial intervensi subsidi 10% terhadap anggaran negara, jumlah keluarga miskin yang terbantu (26,36 juta jiwa berdasarkan BPS 2024), dan estimasi penurunan angka inflasi pangan.

### 3. Early Warning System Berbasis Ambang Batas Statistik (Baru)

Sistem tidak hanya memprediksi nominal harga ke depan, tetapi juga mengkalkulasi ambang batas kerawanan secara dinamis per komoditas menggunakan rumus:

* **Waspada:** $\text{Mean} + 1.5 \times \text{StdDev}$
* **Bahaya:** $\text{Mean} + 2.0 \times \text{StdDev}$

Jika angka proyeksi menembus batas tersebut, sistem secara otomatis melabeli status menjadi `WASPADA` atau `BAHAYA` dan melemparnya ke dalam *array* peringatan dini global.

### 4. Tracker Transparansi Akurasi Model (Baru)

Menampilkan metrik performa MAE dan rata-rata MAPE secara transparan di dashboard (misalnya rata-rata MAPE saat ini berada di kisaran ~11.09%), memvalidasi keandalan prediksi sebelum digunakan oleh pengambil keputusan kebijakan.

## Pembenaran & Optimalisasi Pipeline (Update Juni 2026)

1. **Self-Healing Consumer & Earliest Offset**:
Thread Kafka consumer dilengkapi dengan *reconnection loop* untuk menahan kendala *network drops* mendadak dan menggunakan strategi `earliest` offset agar data historis pasca-*downtime* tidak hilang.
2. **Sanitasi Pivot Kolom Eksplisit di Spark**:
Pembersihan data `None`/null secara ketat sebelum proses agregasi korelasi. Struktur pivot menggunakan daftar komoditas eksplisit (`komoditas_list`) untuk menghindari fenomena hilangnya baris data secara menyeluruh setelah fungsi `.dropna()`.
3. **Mesin Prediksi GBT Robust dengan Fallback Linear**:
Jika sebaran data pada time series lokal atau HDFS sangat minim atau tidak berdistribusi normal yang memicu kegagalan latih pada pohon keputusan GBT, eksekusi Spark dilindungi oleh blok *multi-try* yang otomatis berpindah ke model regresi linier biasa.
4. **Sinkronisasi Volume & Keamanan I/O Lingkungan Terisolasi**:
Untuk mengatasi pembatasan hak akses sistem berkas kontainer Docker (`unlinkat/open permission denied`) pada *environment* Cloud Shell, alur pembaruan dashboard dipindahkan menggunakan skema penyalinan administratif berkala (`sudo docker cp` diikuti penyesuaian kepemilikan lewat `chown`), yang menjamin kelancaran pembacaan data *real-time*.

## Cara Menjalankan Manual & Troubleshooting

### Trigger Jalur Spark Manual via Kontainer

Jika Anda ingin memaksa pembaruan matriks ML dan perhitungan *alerts* saat ini juga tanpa menunggu penjadwalan otomatis 2 menit, jalankan urutan perintah ini:

```bash
# Salin kode analisis terbaru ke dalam master node Spark
docker cp spark/analysis.py spark_master_pangan:/spark-apps/analysis.py

# Eksekusi pipeline submit Spark ML
docker exec -it spark_master_pangan /opt/spark/bin/spark-submit --master spark://spark-master:7077 /spark-apps/analysis.py

# Tarik data json hasil pemodelan kembali ke Cloud Shell
sudo docker cp spark_master_pangan:/dashboard/data/spark_results.json dashboard/data/spark_results.json
sudo chown $(whoami):$(whoami) dashboard/data/spark_results.json

```

atau

```
# Jalankan Spark analysis dengan path lokal
docker exec -it spark_master_pangan bash -c \
  "cd /spark-apps && LOCAL_INPUT_DIR=/data/output OUTPUT_DIR=/data/output \
  /opt/spark/bin/spark-submit --master local[2] analysis.py"
```

### Menghentikan Seluruh Sistem

```bash
docker compose down
docker compose down -v   <- Reset total seluruh volume HDFS dan Kafka

>>>>>>> fab2d29 (fix)
```

---

<<<<<<< HEAD
## Docker Services

| Service | Image | Port | Akses UI |
|---------|-------|------|----------|
| Kafka (KRaft) | confluentinc/cp-kafka:7.6.1 | 9092 | — |
| HDFS NameNode | bde2020/hadoop-namenode | 19870, 8020 | http://localhost:19870 |
| HDFS DataNode | bde2020/hadoop-datanode | — | — |
| YARN ResourceManager | bde2020/hadoop-resourcemanager | 8088 | — |
| Spark Master | apache/spark:3.5.0 | 8080, 7077 | http://localhost:8080 |
| Spark Worker | apache/spark:3.5.0 | — | — |
| Flask Dashboard | fp-flask | 5002 | **http://localhost:5002** |
| Kafka Consumer | fp-consumer | — | — |

---

## Detail Model Machine Learning

### GBTRegressor (Gradient Boosted Trees)

Model utama yang digunakan untuk prediksi harga per komoditas.

**Hyperparameter:**

| Parameter | Nilai | Penjelasan |
|-----------|-------|------------|
| `maxIter` | 20 | Jumlah pohon keputusan (boosting rounds) |
| `maxDepth` | 3 | Kedalaman maksimum setiap pohon — mencegah overfitting |
| `stepSize` | 0.1 | Learning rate — seberapa cepat model belajar dari error sebelumnya |
| `featuresCol` | `features` | Kolom input dari VectorAssembler |
| `labelCol` | `label` (= harga_avg) | Kolom target yang diprediksi |

**Fitur Input (8 fitur):**

| Fitur | Tipe | Sumber | Alasan |
|-------|------|--------|--------|
| `kurs_avg` | Numerik | yfinance | Sinyal utama dampak impor |
| `kurs_7d_avg` | Numerik | Rolling window 7 hari | Tren jangka pendek kurs |
| `flag_lebaran` | Binary | Kalender Indonesia | Lonjakan permintaan musiman |
| `flag_panen` | Binary | Kalender pertanian | Penurunan harga musim panen |
| `flag_impor_ekspor` | Binary | Event kalender | Kebijakan perdagangan |
| `lag_1` | Numerik | Harga hari sebelumnya | Autokorelasi harga |
| `sentiment_score` | Numerik (-1/0/+1) | Rata-rata berita RSS | Sinyal tekanan pasar |
| `lag_7` | Numerik* | Harga 7 hari lalu | Pola mingguan (jika data cukup) |

*`lag_7` hanya ditambahkan jika data historis ≥ 8 hari.

**Cara model mempelajari kurs:**
Model GBT membangun ensemble pohon keputusan secara berurutan. Setiap pohon baru memperbaiki error pohon sebelumnya (gradient descent pada ruang fungsi). Fitur `kurs_avg` dan `kurs_7d_avg` dimasukkan bersama fitur lag harga — model secara otomatis belajar *seberapa besar* perubahan kurs memengaruhi harga masing-masing komoditas, yang berbeda-beda tergantung proporsi impornya (gandum 100% impor lebih sensitif terhadap kurs dibanding cabai yang lokal).

**Sistem Peringatan Dini:**

```
Threshold WASPADA = mean_harga_30hari + 1.5 × stddev_harga_30hari
Threshold BAHAYA  = mean_harga_30hari + 2.0 × stddev_harga_30hari

Jika prediksi_t+7 ≥ threshold → status = WASPADA / BAHAYA
```

**Metrik Evaluasi:**
- **MAE (Mean Absolute Error):** Rata-rata selisih absolut prediksi vs aktual dalam Rupiah
- **MAPE (Mean Absolute Percentage Error):** Persentase error relatif (target < 15%)

**Fallback ke LinearRegression:**
Jika GBT gagal melatih pohon (data terlalu sedikit atau tidak bervariasi), sistem otomatis beralih ke LinearRegression (`maxIter=100, regParam=0.01`) agar dashboard tidak blank.

---

## Cara Kerja LLM Integration

Endpoint `/api/rekomendasi` menggunakan chain:

```
Alert aktif (WASPADA/BAHAYA)
          │
          ▼
  Build prompt kontekstual
  (komoditas + prediksi + berita relevan + kurs)
          │
          ▼
  [1] Claude API (claude-sonnet-4-6)   ← Primary
          │ gagal / no API key
          ▼
  [2] Gemini API (gemini-2.0-flash)    ← Fallback 1
          │ gagal / no API key
          ▼
  [3] Rule-Based Template              ← Fallback 2 (selalu berhasil)
          │
          ▼
  Cache 30 menit
          │
          ▼
  Tampil di dashboard
```

Contoh output LLM:
> "Beras IR I diprediksi naik 6,2% dalam 14 hari menjadi Rp14.800/kg. Penyebab utama: pelemahan rupiah ke Rp18.209/USD meningkatkan biaya impor beras. Rekomendasi: percepat realisasi impor cadangan beras pemerintah (CBP) sebesar 300.000 ton, aktifkan operasi pasar Bulog di 10 kota besar, tinjau kembali HET beras medium."

---

## Langkah Pengerjaan

### Langkah 1 — Persiapan Infrastruktur

1. Clone repository dan pastikan Docker Desktop berjalan
2. Buat file `.env` di folder `dashboard/` untuk API key LLM (opsional):
   ```
   ANTHROPIC_API_KEY=sk-ant-xxx   # untuk Claude
   GEMINI_API_KEY=AIzaXXX          # atau Gemini sebagai fallback
   ```
3. Jalankan semua service:
   ```bash
   docker compose up --build -d
   ```
4. Tunggu ~30 detik, verifikasi semua container jalan:
   ```bash
   docker compose ps
   ```

### Langkah 2 — Inisialisasi Kafka Topics & HDFS

```bash
docker exec -it kafka /bin/bash -c \
  "kafka-topics --create --topic pangan_data --bootstrap-server localhost:9092 --partitions 3 && \
   kafka-topics --create --topic pangan_berita --bootstrap-server localhost:9092 --partitions 2"
```

Atau gunakan script otomatis:
```bash
python scripts/init_infrastructure.py
```

### Langkah 3 — Menjalankan Data Pipeline

Di tiga terminal terpisah (atau otomatis via Docker):

**Terminal 1 — Producer harga & kurs:**
```bash
docker exec -it kafka_consumer_pangan python /app/kafka/producer_api.py
```

**Terminal 2 — Producer berita & sentimen:**
```bash
docker exec -it kafka_consumer_pangan python /app/kafka/producer_rss.py
```

**Terminal 3 — Consumer ke HDFS:**
```bash
docker exec -it kafka_consumer_pangan python /app/kafka/consumer_to_hdfs.py
```

Setelah berjalan ~2 menit, cek data sudah masuk:
- HDFS UI: http://localhost:19870 → navigasi ke `/data/pangan/`
- File lokal: `dashboard/data/live_summary.json` sudah terisi

### Langkah 4 — Menjalankan Analisis Spark ML

```bash
# Submit Spark job (otomatis jalan tiap 2 menit via scheduler)
docker cp spark/analysis.py spark_master_pangan:/spark-apps/analysis.py
docker exec -it spark_master_pangan \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /spark-apps/analysis.py

# Tarik hasil ke dashboard
sudo docker cp spark_master_pangan:/dashboard/data/spark_results.json \
  dashboard/data/spark_results.json
```

Cek Spark Master UI di http://localhost:8080 untuk melihat job yang selesai.

### Langkah 5 — Validasi & Dashboard

Buka browser, akses http://localhost:5002

Pastikan semua section terisi:
- ✅ Harga pangan live (dari `live_summary.json`)
- ✅ Prediksi 7/14/30 hari dengan badge AMAN/WASPADA/BAHAYA
- ✅ Alert panel aktif
- ✅ Panel rekomendasi LLM (klik "Perbarui Rekomendasi")
- ✅ Line chart historis + prediksi

Cek health endpoint:
```bash
curl http://localhost:5002/api/health
```

---

## Cara Menjalankan

### Otomatis (Docker)

```bash
# Kloning dan jalankan
git clone [repo-url]
cd fp-bigdata-kelompok8

# (Opsional) Buat file .env untuk LLM
echo "ANTHROPIC_API_KEY=sk-ant-xxx" > dashboard/.env
echo "GEMINI_API_KEY=AIzaXXX" >> dashboard/.env

# Jalankan semua service
docker compose up --build -d

# Tunggu ~60 detik, lalu akses:
# http://localhost:5002  ← Dashboard utama
# http://localhost:19870 ← HDFS NameNode UI
# http://localhost:8080  ← Spark Master UI
```

### Manual (Trigger Spark)

Jika ingin paksa update analisis Spark sekarang:

```bash
# Salin analysis.py ke container
docker cp spark/analysis.py spark_master_pangan:/spark-apps/analysis.py

# Submit Spark job
docker exec -it spark_master_pangan \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /spark-apps/analysis.py

# Tarik hasil ke dashboard
sudo docker cp spark_master_pangan:/dashboard/data/spark_results.json \
  dashboard/data/spark_results.json
```

### Menghentikan

```bash
docker compose down        # stop services
docker compose down -v     # stop + hapus semua volume (HDFS + Kafka)
```

---

## Output JSON Utama (`spark_results.json`)

```json
{
  "timestamp": "2026-06-27T12:00:00",
  "korelasi_kurs": [
    { "komoditas": "Gandum", "korelasi": 0.87, "interpretasi": "Sangat Kuat" }
  ],
  "volatilitas": [
    { "komoditas": "Cabai Rawit Merah", "volatilitas_pct": 18.4 }
  ],
  "prediksi": [
    {
      "komoditas": "Beras IR I",
      "model": "GBTRegressor",
      "harga_saat_ini": 13950,
      "prediksi_7h": 14200,
      "prediksi_14h": 14450,
      "prediksi_30h": 14900,
      "mae": 320.5,
      "mape_pct": 2.3,
      "alerts": {
        "t7":  { "hari_ke": 7,  "prediksi": 14200, "threshold_waspada": 15000, "status": "AMAN" },
        "t14": { "hari_ke": 14, "prediksi": 14450, "threshold_waspada": 15000, "status": "AMAN" },
        "t30": { "hari_ke": 30, "prediksi": 14900, "threshold_waspada": 15000, "status": "AMAN" }
      }
    }
  ],
  "evaluasi_model": {
    "n_komoditas_diproses": 7,
    "rata_rata_mae": 890.2,
    "rata_rata_mape_pct": 3.8
  },
  "simulasi_subsidi": { ... },
  "heatmap": { ... }
}
```

---

## Pembeda dari Sistem Sejenis

| Fitur | Proyek Ini |
|-------|-----------|
| Prediksi multi-horizon | ✅ 7/14/30 hari |
| Fitur musiman otomatis | ✅ Lebaran/panen/impor |
| Sentimen berita real-time | ✅ Dari 5 portal |
| Alert berbasis statistik | ✅ mean ± 1.5σ / 2.0σ |
| Rekomendasi LLM | ✅ Claude/Gemini |
| Simulasi kebijakan | ✅ Subsidi 10% + dampak anggaran |
| Transparansi model | ✅ MAE + MAPE live |

---

## Sumber Data

- **Harga Pangan:** [hargapangan.id](https://hargapangan.id) (via API scraping)
- **Kurs USD/IDR:** [yfinance](https://pypi.org/project/yfinance/) (USDIDR=X, real-time)
- **Berita:** RSS Feed Kontan, Kompas Ekonomi, Bisnis.com, Detik Finance, Tempo Bisnis
- **Referensi Kemiskinan:** BPS 2024 — 26,36 juta keluarga miskin
- **Referensi Impor:** Kementan 2024, BPS Trade Statistics

---

*Auto-refresh dashboard setiap 30 detik. Rekomendasi LLM di-cache 30 menit.*
=======
## Keterbatasan & Catatan

- **Prediksi horizon**: GBTRegressor membutuhkan minimal 3 hari data 
  timeseries per komoditas untuk menghasilkan prediksi yang divergen 
  antar horizon t+7/14/30. Sistem baru berjalan akan menampilkan 
  nilai yang sama — ini normal dan bukan bug.

- **Sumber harga**: hargapangan.id dan panelharga.badanpangan.go.id 
  dapat mengalami downtime. Sistem otomatis fallback ke model korelasi 
  empiris berbasis harga referensi PIHPS BI Juni 2026 + kurs real yfinance.

- **HDFS**: DataNode dan ResourceManager dalam kondisi restart loop 
  karena keterbatasan resource WSL. Data tetap tersimpan di file lokal 
  (dashboard/data/) yang di-mount ke container Flask dan Spark.

- **LLM Rekomendasi**: Endpoint /api/rekomendasi memanggil Claude API 
  → Gemini API → rule-based fallback secara berurutan. Tanpa API key, 
  sistem tetap berjalan dengan rekomendasi rule-based.
>>>>>>> fab2d29 (fix)
