# WeatherPulse — Big Data Weather Pipeline

**Kelompok 5 Big Data A**

| NRP | Nama | Kontribusi |
|-----|------|------------|
| 5027241027 | Yuan Banny | Ingest + HDFS |
| 5027241103 | Ni'mah Fauziyyah | Kafka |
| 5027241093 | Atha Tajuddin | PySpark |
| 5027241002 | Balqis Sani S | Dashboard |
| 5027241004 | Mey Rosalina | Dashboard |

---

## Arsitektur

```
Open-Meteo API ─┐
                ├─► Producer ─► Kafka ─► Consumer ─► HDFS
RSS (Antara,    ─┘                                     │
 Mongabay)                                             ▼
                                                  Bronze (Delta)
                                                       │
                                                  Silver (Delta)
                                                       │
                                                 Spark Analysis
                                                       │
                                                   Gold (Delta)
                                                       │
                                                   Dashboard
```

**Stack:** Apache Kafka (KRaft) · Apache Hadoop HDFS · Apache Spark + Delta Lake · Flask

**Kota dipantau:** Jakarta · Surabaya · Semarang · Medan · Makassar · Denpasar

---

## Struktur Direktori

```
.
├── hadoop/             # Dockerfile + config HDFS cluster
├── kafka/              # Dockerfile Kafka broker (KRaft)
├── medallion/          # Pipeline Spark: bronze, silver, spark_analysis, gold
│   ├── 01_bronze.py    # Ingest HDFS → Delta
│   ├── 02_silver.py    # Cleaning & normalisasi
│   ├── spark_analysis.py  # 3 analisis dari Silver
│   ├── 03_gold.py      # Business tables → Gold Delta
│   └── lakehouse_data/ # Mount point untuk Delta tables
├── dashboard/          # Flask app (port 5000)
├── producer_api.py     # Kirim data cuaca ke Kafka
├── producer_rss.py     # Kirim berita RSS ke Kafka
├── consumer_to_hdfs.py # Simpan Kafka → HDFS (flush tiap 2 menit)
├── config.py           # Konfigurasi global (HDFS, Kafka, kota)
├── main.ipynb          # Notebook interaktif (dev/testing)
└── docker-compose.yml  # Satu file untuk seluruh stack
```

---

## Cara Menjalankan

### Prasyarat
- Docker & Docker Compose

### Pertama kali (clean start)
```bash
docker compose down -v
docker compose up --build
```

`down -v` diperlukan untuk menghapus data lama Kafka (format ZooKeeper) agar bisa start dalam mode KRaft.

### Setelah pertama kali
```bash
docker compose up
```

### Urutan service yang berjalan otomatis
1. **Hadoop** (namenode healthy → datanode → resourcemanager → nodemanager)
2. **Kafka** (KRaft, healthcheck aktif)
3. **producer-api + producer-rss** → mulai kirim data ke Kafka
4. **consumer-hdfs** → mulai flush data ke HDFS setiap 2 menit
5. **medallion-bronze** → tunggu data HDFS siap, ingest ke Delta
6. **medallion-silver** → cleaning & normalisasi
7. **spark-analysis** → 3 analisis (suhu, ekstrem, tren jam)
8. **medallion-gold** → simpan business tables ke Gold Delta
9. **dashboard** → buka `http://localhost:5000`

---

## Cek Status

```bash
# Lihat semua container
docker compose ps

# Log salah satu service
docker compose logs -f medallion-bronze

# Cek isi HDFS
docker exec hadoop-namenode hdfs dfs -ls /data/weather/api

# Cek data di Kafka
docker exec kafka-broker kafka-topics.sh --bootstrap-server localhost:9092 --list

docker compose run --rm --no-deps medallion-bronze python3 01_bronze.py

docker compose run --rm --no-deps medallion-silver python3 02_silver.py

docker compose run --rm --no-deps medallion-gold   python3 03_gold.py

```

---

## Data Paths

| Layer | Path |
|-------|------|
| HDFS raw API | `/data/weather/api/` |
| HDFS raw RSS | `/data/weather/rss/` |
| Bronze Delta | `/lakehouse/bronze/weather_api`, `/lakehouse/bronze/weather_rss` |
| Silver Delta | `/lakehouse/silver/weather_api`, `/lakehouse/silver/weather_rss` |
| Gold Delta | `/lakehouse/gold/weather_analytics`, `/lakehouse/gold/weather_extremes`, dll |
| Dashboard JSON | `dashboard/data/spark_results.json` |

---

*ETS Big Data 2026 — Kelompok 5A*
