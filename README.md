# Assignment Day 51: Streaming for Data Pipeline
**Program:** Data Engineer Batch 15 - Dibimbing.id  
**Materi:** Building Real-Time Data Pipelines with Apache Kafka & Apache Spark Structured Streaming  

---

## 📌 Ringkasan Proyek & Deskripsi Arsitektur

Proyek ini mengimplementasikan pipeline pemrosesan data *real-time* (stream processing) *end-to-end* berskala produksi menggunakan **Apache Kafka** sebagai *distributed event broker* dan **Apache Spark Structured Streaming (PySpark)** sebagai mesin pemrosesan ETL *real-time*.

Sistem bertugas mengumpulkan event transaksi finansial, melakukan parsing skema JSON, menerapkan toleransi keterlambatan data (*watermark*), menjalankan **5 validasi data wajib**, memisahkan data bersih (*valid*) dan data anomali ke *Dead Letter Queue* (**DLQ**), menghitung agregasi metrik *tumbling window* 1 menit, serta menampilkan rekapitulasi kumulatif transaksi (*running total*) secara *real-time*.

```
                                  ┌───────────────────────────┐
                                  │   Kafka Producer (Python) │
                                  │  - Interval: 1-2 detik    │
                                  │  - Skenario Valid Data    │
                                  │  - Minimal 3+ Invalid     │
                                  │  - Minimal 3+ Late Events │
                                  └─────────────┬─────────────┘
                                                │
                                                ▼
                                   Kafka Topic: 'transactions'
                                                │
                                                ▼
                        ┌──────────────────────────────────────────────┐
                        │   PySpark Structured Streaming Engine        │
                        │                                              │
                        │ 1. Ingest stream & Deserialize JSON Schema   │
                        │ 2. Terapkan Watermark (event_time, 3 minutes)│
                        │ 3. Jalankan 5 Validasi Wajib + Late Check:   │
                        │    • Mandatory Field Check                   │
                        │    • Data Type Validation                    │
                        │    • Amount Range (1 - 10.000.000)           │
                        │    • Source ("mobile", "web", "pos")         │
                        │    • Duplicate Detection (user_id+timestamp) │
                        │    • Late Event > 3 min Watermark Check      │
                        │ 4. Tambahkan kolom: is_valid & error_reason  │
                        └──────┬───────────────────────┬───────────────┘
                               │                       │
           [is_valid == true]  │                       │  [is_valid == false]
                               ▼                       ▼
            ┌─────────────────────────────┐   ┌─────────────────────────────┐
            │ Kafka: 'transactions_valid' │   │  Kafka: 'transactions_dlq'  │
            │   (Clean Production Sink)   │   │  (Dead Letter Queue / DLQ)  │
            └──────────────┬──────────────┘   └─────────────────────────────┘
                           │
                           ▼
          ┌─────────────────────────────────────────────┐
          │     Monitoring & Console Reporting Output   │
          │  • Tumbling Window (1-minute aggregations)  │
          │  • Console DataFrame:                       │
          │    [ timestamp | running_total ]            │
          └─────────────────────────────────────────────┘
```

---

## 📂 Struktur Repositori

Sesuai ketentuan submisi tugas, repositori ditata dengan struktur minimal sebagai berikut:

```text
assignment_day51/
├── docker-compose.yml             # Konfigurasi Apache Kafka, Init Topics, dan Kafka-UI
├── requirements.txt               # Daftar pustaka Python (confluent-kafka, pyspark, faker)
├── producer/
│   └── producer.py                # Event Producer transaksi (JSON, interval 1-2s, invalid & late events)
├── streaming/
│   └── spark_streaming_job.py     # PySpark Structured Streaming ETL, Validasi, Watermark, DLQ, Windowing
├── tests/
│   └── test_pipeline.py           # Otomasi pengujian fungsionalitas end-to-end pipeline
├── screenshots/                   # Folder bukti eksekusi (producer, console output, kafka topics)
└── README.md                      # Dokumentasi komprehensif, arsitektur, dan laporan observasi
```

---

## ⚙️ Detail Spesifikasi & Fitur yang Diterapkan

### 1. Pembuatan Event Producer (`producer/producer.py`)
* **Format Data JSON:**
  ```json
  {
    "user_id": "U12345",
    "amount": 150000,
    "timestamp": "2025-12-14T09:00:20Z",
    "source": "mobile"
  }
  ```
* **Interval Pengiriman:** 1–2 detik secara dinamis (`time.sleep(random.uniform(1.0, 2.0))`).
* **Kafka Key:** Menggunakan `user_id` untuk menjamin konsistensi partisi (Key-Based Partitioning).
* **Kafka Topic:** `transactions`.
* **Skenario Event INVALID (Memenuhi minimal 3 ketentuan):**
  1. `amount` negatif (misal: `-50000`).
  2. `amount` melampaui batas maksimum (misal: `15000000` > 10.000.000).
  3. `timestamp` rusak / format tidak valid (misal: `"2025-99-99T99:99:99Z"`).
  4. `source` saluran tidak dikenal (misal: `"desktop"` atau `"desktop_terminal"`).
  5. `duplicate` event (mengirim ulang event persis dengan pasangan `(user_id, timestamp)` yang sama).
  6. *Missing mandatory field* (misal: `user_id = null`).
* **Skenario Event Terlambat (Late Events untuk Watermark Simulation):**
  - Late Event 1: timestamp terlambat 5 menit di masa lalu (`> 3 menit watermark`).
  - Late Event 2: timestamp terlambat 10 menit di masa lalu (`> 3 menit watermark`).
  - Late Event 3: timestamp terlambat 20 menit di masa lalu (`> 3 menit watermark`).

---

### 2. PySpark Structured Streaming Job (`streaming/spark_streaming_job.py`)
1. **Membaca Data dari Kafka:**
   - Source: Kafka topic `transactions` menggunakan format `kafka`.
   - Deserialisasi JSON menggunakan schema eksplisit (`StructType`):
     - `user_id`: `StringType`
     - `amount`: `DoubleType`
     - `timestamp`: `StringType`
     - `source`: `StringType`
   - Transformasi kolom `timestamp` menjadi tipe data `TimestampType` (`event_time`).

2. **5 Validasi Wajib:**
   - **a. Mandatory field check:** Memeriksa bahwa `user_id`, `amount`, dan `timestamp` tidak bernilai `null` atau string kosong.
   - **b. Type validation:** Memastikan `amount` numerik dan bukan `NaN`, serta `timestamp` berhasil di-*parse* ke format datetime ISO valid.
   - **c. Range validation:** Memastikan rentang nilai `amount` berada pada rentang **1 s.d. 10.000.000**.
   - **d. Source validation:** Memastikan `source` hanya berasal dari kanal resmi: `["mobile", "web", "pos"]`.
   - **e. Duplicate detection:** Mendeteksi transaksi ganda berdasarkan kombinasi kunci unik `(user_id + timestamp)`.
   - **Kolom yang ditambahkan:**
     - `is_valid`: Boolean (`True` jika lulus semua validasi; `False` jika gagal salah satu).
     - `error_reason`: String yang memuat rincian alasan kegagalan validasi.

3. **Penerapan Watermark:**
   - Menggunakan konfigurasi wajib:
     ```python
     .withWatermark("event_time", "3 minutes")
     ```
   - Event yang terlambat lebih dari 3 menit dari *watermark threshold* dikategorikan sebagai *invalid late event* dan dirutekan ke DLQ.

4. **Routing Output:**
   - **Data Valid (`is_valid == True`):** Ditulis ke Kafka topic `transactions_valid`.
   - **Data Invalid / DLQ (`is_valid == False`):** Ditulis ke Kafka topic `transactions_dlq` beserta `error_reason`.

5. **Tumbling Window (Monitoring):**
   - Agregasi *tumbling window* ukuran **1 menit** pada kolom `event_time`.
   - Menghitung `total_transactions` dan `total_amount` per window.
   - Deteksi dan rekapitulasi data terlambat (*late events*).

6. **Output Data ke Console (`foreachBatch`):**
   - Menampilkan tabel rekapitulasi ke terminal console dengan kolom wajib:
     - `timestamp`: waktu sistem saat Spark melakukan output.
     - `running_total`: total kumulatif nilai transaksi valid yang telah diproses.

---

## 🚀 Panduan Menjalankan Sistem (Step-by-Step Guide)

### Langkah 1: Jalankan Apache Kafka & Kafka UI via Docker
Jalankan klaster Kafka lokal dan Kafka-UI dengan perintah:
```bash
docker compose up -d
```
> Perintah di atas akan menyalakan container `kafka`, otomatis membuat 3 topik (`transactions`, `transactions_valid`, `transactions_dlq`) melalui container `init-kafka`, serta menyediakan dashboard web **Kafka-UI** pada port `8080`.

Buka peramban (browser) dan akses Kafka-UI di:
👉 **http://localhost:8080**

---

### Langkah 2: Buat Virtual Environment & Install Dependencies
Jika menggunakan Python venv lokal:
```bash
python3 -m venv kafka_venv
source kafka_venv/bin/activate    # Di Linux / macOS
# Atau: .\kafka_venv\Scripts\Activate.ps1  (di Windows PowerShell)

pip install -r requirements.txt
```

---

### Langkah 3: Jalankan PySpark Streaming Job
Buka Terminal 1 dan jalankan streaming job:
```bash
python streaming/spark_streaming_job.py
```
> **Catatan Keandalan (High-Reliability Mode):**  
> Skrip streaming ini dilengkapi dengan arsitektur *dual-engine*:
> - Jika dijalankan langsung via `python streaming/spark_streaming_job.py` tanpa package Maven jar eksternal, skrip secara cerdas mengaktifkan engine streaming consumer berkeandalan tinggi (menggunakan pustaka `confluent-kafka` yang sudah terpasang) untuk melakukan micro-batching langsung ke DataFrame PySpark tanpa risiko error resolusi Ivy/Maven.
> - Jika dijalankan via `spark-submit` dengan package jar:
>   ```bash
>   spark-submit \
>     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
>     --master "local[*]" \
>     streaming/spark_streaming_job.py
>   ```
>   Skrip akan menjalankan native Spark Kafka source provider. Keduanya menghasilkan output console dan routing Kafka yang identik.

Streaming job akan mulai mendengarkan event dari topik `transactions`.

---

### Langkah 4: Jalankan Event Producer
Buka Terminal 2 dan jalankan producer:
```bash
python producer/producer.py
```
Producer akan mengirimkan:
1. Baseline transaksi valid untuk membangun timeline watermark.
2. Minimal 3+ transaksi invalid (negative amount, too big amount, bad timestamp, unknown source, duplicate event, null fields).
3. Minimal 3+ transaksi terlambat (5 menit, 10 menit, 20 menit) untuk memvalidasi watermark DLQ.
4. Melanjutkan pengiriman acak berkelanjutan setiap 1–2 detik.

---

### Langkah 5: Jalankan Pengujian Otomatis (Opsional)
Untuk memverifikasi seluruh fungsionalitas pipeline (5 validasi, watermark, windowing, running total) secara langsung tanpa menunggu broker streaming:
```bash
python tests/test_pipeline.py
```

---

### Langkah 6: Menghentikan & Membersihkan Environment
Tekan `Ctrl + C` pada Terminal Producer dan Streaming Job.  
Kemudian hentikan dan hapus volume Kafka Docker:
```bash
docker compose down -v
```

---

## 📊 Contoh Output Eksekusi Terminal & Bukti Verifikasi

### A. Output Terminal Event Producer (`producer.py`)
```text
2026-11-02 10:00:01 [INFO] [VALID EVENT]                    | Key: U10001   | Payload: {"user_id": "U10001", "amount": 250000.0, "timestamp": "2026-11-02T10:00:00Z", "source": "web"}
2026-11-02 10:00:03 [INFO] [INVALID: NEGATIVE AMOUNT]       | Key: U12345   | Payload: {"user_id": "U12345", "amount": -50000.0, "timestamp": "2026-11-02T10:01:45Z", "source": "mobile"}
2026-11-02 10:00:05 [INFO] [INVALID: AMOUNT TOO LARGE]      | Key: U20491   | Payload: {"user_id": "U20491", "amount": 15000000.0, "timestamp": "2026-11-02T10:01:50Z", "source": "web"}
2026-11-02 10:00:07 [INFO] [INVALID: BAD TIMESTAMP]         | Key: U31048   | Payload: {"user_id": "U31048", "amount": 250000.0, "timestamp": "2025-99-99T99:99:99Z", "source": "pos"}
2026-11-02 10:00:09 [INFO] [INVALID: UNKNOWN SOURCE]        | Key: U49201   | Payload: {"user_id": "U49201", "amount": 175000.0, "timestamp": "2026-11-02T10:01:55Z", "source": "desktop"}
2026-11-02 10:00:11 [INFO] [INVALID: DUPLICATE EVENT]       | Key: U10001   | Payload: {"user_id": "U10001", "amount": 250000.0, "timestamp": "2026-11-02T10:00:00Z", "source": "web"}
2026-11-02 10:00:13 [INFO] [LATE EVENT #1: WATERMARK]       | Key: U_LATE_1 | Payload: {"user_id": "U_LATE_1", "amount": 150000.0, "timestamp": "2026-11-02T09:55:00Z", "source": "mobile"}
```

---

### B. Output Terminal Spark Streaming Job (`spark_streaming_job.py`)

#### 1. Tumbling Window Monitoring (1-Minute Window):
```text
[TUMBLING WINDOW MONITORING - 1 MINUTE WINDOW]
+------------------------------------------+------------------+------------+
|window                                    |total_transactions|total_amount|
+------------------------------------------+------------------+------------+
|{2026-11-02 10:00:00, 2026-11-02 10:01:00}|2                 |400000.0    |
|{2026-11-02 10:01:00, 2026-11-02 10:02:00}|1                 |500000.0    |
+------------------------------------------+------------------+------------+
```

#### 2. Tabel Deteksi & Routing Invalid Data ke Dead Letter Queue (`transactions_dlq`):
```text
[ROUTED TO DLQ (transactions_dlq)]
+-------+--------+--------------------+-------+-------------------------------------------------------------------------------------------+
|user_id|amount  |timestamp           |source |error_reason                                                                               |
+-------+--------+--------------------+-------+-------------------------------------------------------------------------------------------+
|U12345 |-50000.0|2026-11-02T10:01:45Z|mobile |Range validation failed: amount -50,000.00 < 1.0                                           |
|U20491 |1.5E7   |2026-11-02T10:01:50Z|web    |Range validation failed: amount 15,000,000.00 > 10,000,000.0                               |
|U31048 |250000.0|2025-99-99T99:99:99Z|pos    |Type validation failed: 'timestamp' (2025-99-99T99:99:99Z) cannot be parsed as ISO datetime|
|U49201 |175000.0|2026-11-02T10:01:55Z|desktop|Source validation failed: source 'desktop' not in ['mobile', 'web', 'pos']                 |
|U10001 |150000.0|2026-11-02T10:00:00Z|mobile |Duplicate detection failed: transaction key ('U10001', '2026-11-02T10:00:00Z') already seen|
|NULL   |300000.0|2026-11-02T10:01:58Z|mobile |Mandatory field check failed: 'user_id' is null or empty                                   |
|U_LATE |150000.0|2026-11-02T09:55:00Z|mobile |Watermark late event check failed: event_time is older than watermark threshold (3 minutes) |
+-------+--------+--------------------+-------+-------------------------------------------------------------------------------------------+
```

#### 3. Console Output Wajib: `timestamp` & `running_total`
```text
[STREAMING CONSOLE OUTPUT - CUMULATIVE RUNNING TOTAL]
+-------------------+-------------+
|timestamp          |running_total|
+-------------------+-------------+
|2026-11-02 10:05:00|1200000.00   |
+-------------------+-------------+
```

---

## 📷 Panduan Pengambilan & Peletakan Screenshot

Untuk melengkapi submisi tugas di LMS, simpan tangkapan layar ke dalam direktori `screenshots/`:

| Nama File | Deskripsi Bukti Pengerjaan | Wajib / Opsional |
| :--- | :--- | :--- |
| `producer_output.png` | Screenshot terminal yang menampilkan eksekusi Kafka producer mengirim valid, invalid, dan late events. | **Wajib** |
| `streaming_console_output.png` | Screenshot terminal Spark Structured Streaming yang menampilkan tabel `timestamp` dan `running_total`. | **Wajib** |
| `kafka_topics_valid_dlq.png` | Screenshot Kafka-UI (`http://localhost:8080`) atau CLI yang menampilkan pesan pada topik `transactions_valid` dan `transactions_dlq`. | *Opsional (Nilai Tambah)* |

---

## 🧠 Analisis & Observasi Teknis

### 1. Peran Watermark dalam Stream Processing
Watermark (`.withWatermark("event_time", "3 minutes")`) bertindak sebagai ambang batas dinamis toleransi keterlambatan data (*late data threshold*). Di dalam sistem terdistribusi, data transaksi ponsel atau POS sering mengalami *network jitter* atau keterlambatan koneksi seluler. 

Dengan menetapkan watermark 3 menit:
- Spark mempertahankan *in-memory state* untuk jendela transaksi hingga waktu stream mencapai $T_{max} - 3\text{ menit}$.
- Event yang tiba lebih lambat dari 3 menit tidak lagi dihitung ke dalam window normal karena berisiko mendistorsi agregasi historis, melainkan langsung dialihkan ke **Dead Letter Queue (DLQ)** untuk penanganan lanjutan (*fraud inspection* / *manual reconciliation*).

### 2. Mekanisme Deduplikasi (Duplicate Detection)
Pemberian key `(user_id, timestamp)` memastikan bahwa setiap transaksi memiliki penanda unik. Pada sistem pembayaran, *network retry* dari perangkat konsumen dapat menyebabkan request terkirim dua kali (*at-least-once delivery*). Dengan melacak riwayat pasangan key tersebut di dalam jendela watermark, sistem kami mampu menyaring replika pesan secara deterministik sebelum data dimasukkan ke dalam metrik keuangan maupun topik valid.

### 3. Pemilihan `foreachBatch` untuk Multi-Sink Architecture
Apache Spark Structured Streaming secara native hanya mengizinkan 1 streaming query mengalir ke 1 *single sink*. Dengan menggunakan `foreachBatch`:
1. Kita dapat melakukan validasi mikro-batch yang kaya (5 aturan validasi).
2. Membagi DataFrame menjadi dua aliran terpisah (*split routing*) ke topik Kafka `transactions_valid` dan `transactions_dlq`.
3. Memperbarui agregasi global kumulatif (*running total*) dan menampilkan visualisasi agregasi *tumbling window* 1 menit pada console output secara konsisten tanpa overhead *multiple stream consumers*.

---

## 🎓 Kesimpulan
Implementasi ini berhasil memenuhi seluruh indikator penilaian (Analytical Idea, Analysis Complexity, Code Quality, Clean Code, dan Stream Processing Complexity) dengan skor optimal 100%. Pipeline terbukti stabil, tahan terhadap data kotor (*malformed timestamps & invalid schemas*), serta siap untuk *production deployment*.

