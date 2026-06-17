# Big Data Analytics - Dampak Kurs Dollar terhadap Harga Pangan Indonesia

Stack: Kafka, HDFS, Spark, Flask | Data: Real-Time | Deploy: Docker

## Latar Belakang

Kurs dolar AS menembus Rp18.209 pada 9 Juni 2026. Indonesia masih mengimpor gandum 10,6 juta ton/tahun, kedelai 2,5 juta ton/tahun, gula 4,4 juta ton/tahun. Sistem ini memantau korelasi kurs USD/IDR dengan 11 komoditas pangan secara real-time.

## Arsitektur

yfinance + hargapangan.id + RSS --> Apache Kafka --> Hadoop HDFS --> Apache Spark --> Flask Dashboard

## Struktur Direktori

```
fp/
+-- docker-compose.yml       <- Semua services Docker
+-- hadoop.env               <- Konfigurasi Hadoop
+-- requirements.txt         <- Python dependencies
+-- run.sh                   <- Script otomatis Linux/Mac
+-- README.md                <- File ini
+-- scripts/
|   +-- init_infrastructure.py  <- Init Kafka topics + HDFS
+-- kafka/
|   +-- producer_api.py      <- Producer kurs + harga pangan
|   +-- producer_rss.py      <- Producer berita RSS
|   +-- consumer_to_hdfs.py  <- Consumer ke HDFS + local
+-- spark/
|   +-- analysis.py          <- 6 analisis Spark
+-- dashboard/
    +-- app.py               <- Flask backend
    +-- Dockerfile
    +-- requirements-flask.txt
    +-- templates/index.html <- Dashboard real-time
    +-- data/                <- Auto-generated saat pipeline jalan
```

## Docker Services

| Komponen         | Image / Source                  | Port         | Fungsi                                       |
|------------------|---------------------------------|--------------|----------------------------------------------|
| Kafka (KRaft)    | confluentinc/cp-kafka:7.6.1     | 9092         | Message broker streaming                     |
| HDFS NameNode    | bde2020/hadoop-namenode         | 19870, 8020  | Storage HDFS Utama (UI: http://localhost:19870) |
| HDFS DataNode    | bde2020/hadoop-datanode         | -            | Node penyimpanan data                        |
| YARN             | bde2020/hadoop-resourcemanager  | 8088         | Resource manager cluster                    |
| Spark Master     | apache/spark:3.5.0              | 8080, 7077   | Spark Master Node                            |
| Spark Worker     | apache/spark:3.5.0              | -            | Spark Worker Node                            |
| Flask Dashboard  | fp-flask (Dockerfile)           | 5002 (Host)  | Dashboard UI Utama (UI: http://localhost:5002) |
| Kafka Consumer   | fp-consumer (Dockerfile.consumer)| -           | Otomatis flush data ke HDFS & file lokal     |

## Data Pipeline & Feature Engineering 
Tiga file di bawah ini diperluas agar menyediakan fitur historis dan sinyal musiman yang dibutuhkan model prediksi dan dashboard.

`kafka/producer_api.py`
- Kurs USD/IDR historis 30 hari dari yfinance (window diperluas dari 2d menjadi 35d, diambil sekali saat startup), dengan fallback simulasi kalau `yfinance` gagal diakses.
- Harga historis 30 hari per komoditas, disimpan sebagai time series rolling (`historis_30h`) di setiap payload.
- Field turunan untuk fitur model: harga_kemarin (lag_1), `kurs_7d_avg`, `kurs_30d_avg`.
- Flag musiman berbasis kalender Indonesia: `flag_lebaran` (mencakup Lebaran, Nataru, Idul Adha), `flag_panen` (dua musim panen raya nasional), `flag_impor_ekspor` (bulan-bulan kebijakan impor aktif), beserta label `musim_event`.

`kafka/producer_rss.py`
- Sentiment scoring sederhana berbasis keyword matching: setiap artikel diberi skor +1 (positif, mis. "panen raya", "stok aman"), 0 (netral), atau -1 (negatif, mis. "gagal panen", "kelangkaan"). Saat keyword positif dan negatif sama-sama muncul, skor negatif diprioritaskan karena lebih kritis untuk peringatan dini.
- Deteksi penyebab dominan per artikel (`penyebab_dominan`): "kurs", "cuaca", "kebijakan", "pasokan", atau "umum", dipakai dashboard untuk menampilkan label penyebab, bukan hanya angka.
- Setiap payload artikel kini menyertakan `sentiment_score`, `sentiment_label`, dan `penyebab_dominan`.

`kafka/consumer_to_hdfs.py`
- Struktur penyimpanan HDFS baru yang ramah time series: `/data/pangan/timeseries/{slug_komoditas}/{tanggal}.json`, terpisah dari struktur batch lama yang tetap dipertahankan untuk kompatibilitas.
- Agregasi harian otomatis per komoditas (harga rata-rata/min/max, kurs rata-rata, flag musiman) yang diperbarui setiap siklus flush.
- Output lokal baru `dashboard/data/timeseries_{komoditas}.json` berisi array ringkasan 30 hari per komoditas, langsung dapat dibaca dashboard tanpa query ke HDFS.
`live_summary.json` diperkaya dengan field dari producer revisi (`harga_kemarin`, `kurs_7d_avg`, `kurs_30d_avg`, flag musiman) agar dashboard bisa menampilkan konteks tambahan di panel harga terkini.

## Cara Menjalankan

### Prasyarat
- Docker Desktop (sudah berjalan)
- RAM minimal 8 GB (direkomendasikan 12-16 GB jika menggunakan WSL2)

### Step 1 - Masuk folder project

Windows PowerShell:
  cd C:\sem4\big-data\fp

Kali Linux / WSL:
  cd /mnt/c/sem4/big-data/fp

### Step 2 - Jalankan Docker containers

Seluruh pipeline (Producers, Kafka, HDFS, Spark, Flask Dashboard, dan Consumer) telah **100% di-Dockerisasi**. Anda hanya perlu menjalankan satu perintah untuk memulai semuanya:

  docker compose up -d

Tunggu semua container running dan healthy (sekitar 30-60 detik):

  docker compose ps

*(Catatan: Semua produser data real-time, consumer, Spark scheduler, dan dashboard Flask akan berjalan otomatis di latar belakang. Saat laptop dimatikan dan dinyalakan kembali, Docker Desktop akan otomatis memulai ulang seluruh layanan ini).*

### Step 3 - Buka Dashboard

Buka browser Anda di alamat berikut untuk melihat dashboard visualisasi utama:
👉 **http://localhost:5002**

*(Jika chart korelasi/volatilitas/heatmap masih kosong di awal, itu karena Spark membutuhkan minimal beberapa batch data terkumpul di HDFS. Biarkan pipeline berjalan selama 1-2 menit).*

### Step 7 - Jalankan Spark Analysis (Otomatis / Manual)

**Otomatis (Sangat Direkomendasikan):**
Sistem telah dilengkapi dengan **Auto-Scheduler** di dalam container Spark Master yang otomatis mengeksekusi Spark-submit setiap **2 menit sekali**. Anda cukup membiarkan pipeline berjalan, dan grafik pada dashboard akan diperbarui secara otomatis.

**Manual (Opsional - Jika ingin langsung memicu update saat ini juga):**
Jika Anda ingin melihat hasil analisis secara instan tanpa menunggu scheduler berjalan, jalankan perintah ini di PowerShell/Terminal:

  docker exec -e OUTPUT_DIR=/data/output -e LOCAL_INPUT_DIR=/data/output spark_master_pangan /opt/spark/bin/spark-submit --master spark://spark-master:7077 /spark-apps/analysis.py

### Menghentikan Pipeline

  docker compose down
  docker compose down -v   <- hapus data volume (reset total)

## Web UI Monitoring

| Service          | URL                     |
|------------------|-------------------------|
| Dashboard Utama  | http://localhost:5002   |
| HDFS NameNode    | http://localhost:19870  |
| Spark Master     | http://localhost:8080   |
| YARN             | http://localhost:8088   |

## Fitur Unggulan (Pembeda dari KursAlert)

### 1. Heatmap Korelasi Antar Komoditas
Matrix korelasi harga semua komoditas menggunakan Spark pivot + stat.corr.
Melihat komoditas mana yang pergerakannya saling terkait.

### 2. Simulasi Dampak Kebijakan Subsidi
Menghitung dampak subsidi 10% per komoditas:
- Keluarga miskin yang terbantu: 26,36 juta jiwa (BPS 2024)
- Dampak terhadap angka inflasi (percentage point)
- Total anggaran yang dibutuhkan per komoditas

Kedua fitur ini TIDAK ADA di KursAlert (mereka fokus UMKM, bukan kebijakan pemerintah).

## Pembenaran & Optimalisasi Pipeline (Update Juni 2026)

Untuk menjamin kelancaran presentasi dan keandalan sistem saat dijalankan dalam waktu lama, beberapa optimalisasi berikut telah diimplementasikan:

1. **Self-Healing Consumer & Earliest Offset**:
   - Ditambahkan mekanisme *reconnection loop* di thread Kafka consumer untuk mencegah *crash* saat broker Kafka mengalami *restart* mendadak atau *network drops*.
   - Offset strategy diubah dari `latest` ke `earliest` agar consumer dapat menarik semua pesan historis yang sempat tertinggal saat consumer dalam kondisi *offline*.

2. **Penyaringan & Pivot Kolom Eksplisit di Spark**:
   - Menambahkan sanitasi otomatis untuk membuang baris data dengan komoditas bernilai `None`/null di Spark.
   - Pivot data di Spark diubah menggunakan parameter eksplisit list komoditas (`komoditas_list`) untuk mencegah terbentuknya kolom `null` virtual yang berujung pada hilangnya seluruh baris data setelah pemanggilan `.dropna()`. Heatmap korelasi kini dijamin selalu muncul.

3. **Penyelarasan Zona Waktu & Aliran Berita RSS Asli**:
   - Seluruh container dikonfigurasi menggunakan zona waktu lokal `Asia/Jakarta` (`TZ: Asia/Jakarta`) sehingga pencatatan log, waktu pengambilan data, dan hasil analisis Spark sinkron dengan waktu WIB host.
   - Aliran berita menyajikan 100% berita real-time asli dari portal media ekonomi (Bisnis.com, CNBC, Detik Finance, Antara) tanpa artikel simulasi, dan dashboard menampilkan waktu publikasi berita asli.

4. **Rolling History Buffer di Consumer**:
   - Mengubah mekanisme penyimpanan JSON lokal di consumer menjadi *rolling history buffer*. File lokal tidak lagi dioverwrite mentah-mentah melainkan digabungkan, disortir berdasarkan timestamp terbaru, dan dibatasi (30 artikel terbaru untuk berita, 200 entri terbaru untuk harga). Hal ini mencegah artikel lama terhapus dari tampilan visual saat flush HDFS berjalan.

## Troubleshooting

Kafka tidak bisa dihubungi:
  docker compose logs kafka --tail=30
  python scripts/init_infrastructure.py

HDFS error:
  docker compose restart namenode datanode

Dashboard kosong:
  Tunggu producer berjalan minimal 1 siklus, lalu refresh browser.

Error import Python (merah di editor):
  pip install -r requirements.txt

Port conflict - pastikan port ini bebas:
  9092, 19870, 8020, 8088, 8080, 7077, 4040, 5002

## Sumber Data

- Kurs USD/IDR: yfinance USDIDR=X (real-time)
- Harga Pangan: hargapangan.id PIHPS Bank Indonesia (scraping)
- Backup Harga: panelharga.badanpangan.go.id
- Berita: RSS Bisnis.com, Kompas Money, CNBC Indonesia, Detik Finance, Antara
- Bobot Pengeluaran: BPS Susenas 2023
- Data Kemiskinan: BPS 2024

## Mata Kuliah

Big Data - Semester 4
Topik: Analisis Dampak Kurs USD/IDR terhadap Harga Pangan Indonesia
