# Big Data Analytics - Dampak Kurs Dollar terhadap Harga Pangan Indonesia

Stack: Kafka, HDFS, Spark, Flask | Data: Real-Time | Deploy: Docker | Repository: Kelompok 8

## Latar Belakang

Kurs dolar AS menembus Rp18.209 pada 9 Juni 2026. Indonesia masih mengimpor gandum 10,6 juta ton/tahun, kedelai 2,5 juta ton/tahun, gula 4,4 juta ton/tahun. Sistem ini memantau korelasi kurs USD/IDR dengan 11 komoditas pangan secara real-time.

## Arsitektur

yfinance + hargapangan.id + RSS --> Apache Kafka --> Hadoop HDFS --> Apache Spark (Machine Learning Upgrade) --> Flask Dashboard

## Struktur Direktori

```
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

### Menghentikan Seluruh Sistem

```bash
docker compose down
docker compose down -v   <- Reset total seluruh volume HDFS dan Kafka

```

---
