# WeatherPulse Pipeline - Panduan Menjalankan

## Struktur File .py

```
.
├── config.py                 # Konfigurasi global (shared)
├── producer_api.py          # Producer: ambil cuaca dari API → Kafka
├── producer_rss.py          # Producer: ambil berita dari RSS → Kafka
├── consumer_to_hdfs.py      # Consumer: Kafka → HDFS
├── spark_analysis.py        # Spark: analisis HDFS → JSON
├── run_pipeline.py          # Orchestration (jalankan semuanya)
├── medallion/
│   ├── 01_bronze.py         # Bronze: raw data (parquet → delta)
│   ├── 02_silver.py         # Silver: transformasi & cleaning
│   ├── 03_gold.py           # Gold: analisis & agregasi
│   └── 04_time_travel.py    # Time travel example (Delta Lake)
└── dashboard/
    └── app.py               # Web dashboard (Flask)
```

## Cara Menjalankan

### Opsi 1: Jalankan Seluruh Pipeline (Recommended)

```bash
python run_pipeline.py
```

Ini akan menjalankan secara berurutan:
1. **Producers** (background) - fetch data cuaca & berita
2. **Consumer** (background) - simpan ke HDFS
3. **Medallion layers** (sequential):
   - Bronze: ingest raw data
   - Silver: clean & transform
   - Gold: aggregate & analyze
4. **Spark Analysis** - analisis final

### Opsi 2: Jalankan Komponen Individual

#### Producer API
```bash
python producer_api.py
```
- Fetch data cuaca setiap 10 menit dari 6 kota
- Kirim ke Kafka topic `weather-api`

#### Producer RSS
```bash
python producer_rss.py
```
- Fetch berita dari 2 RSS feed
- Kirim ke Kafka topic `weather-rss` setiap 5 menit

#### Consumer
```bash
python consumer_to_hdfs.py
```
- Baca dari Kafka topics
- Batch & flush ke HDFS setiap 2 menit
- Paths: `/data/weather/api/` dan `/data/weather/rss/`

#### Medallion Layers
```bash
# Bronze: raw data → parquet/delta
python medallion/01_bronze.py

# Silver: clean & deduplicate
python medallion/02_silver.py

# Gold: analisis & agregasi
python medallion/03_gold.py
```

#### Spark Analysis
```bash
python spark_analysis.py
```
- Baca dari HDFS
- Jalankan 3 analisis:
  1. Statistik suhu per kota
  2. Deteksi kondisi ekstrem
  3. Tren suhu per jam
- Output: `dashboard/data/spark_results.json`

## Data Flow

```
Open-Meteo API
      ↓
producer_api.py
      ↓
Kafka (weather-api)
      ↓
consumer_to_hdfs.py
      ↓
HDFS (/data/weather/api/)
      ↓
medallion/01_bronze.py
      ↓
/lakehouse/bronze/
      ↓
medallion/02_silver.py
      ↓
/lakehouse/silver/
      ↓
medallion/03_gold.py
      ↓
/lakehouse/gold/
      ↓
spark_analysis.py
      ↓
dashboard/data/spark_results.json
      ↓
dashboard/app.py (web UI)
```

## Konfigurasi

Edit `config.py` untuk mengubah:
- `KOTA` - daftar kota yang dimonitor
- `RSS_URLS` - RSS feeds untuk berita
- `INTERVAL_API` - interval fetch API (default: 600s = 10 min)
- `INTERVAL_RSS` - interval fetch RSS (default: 300s = 5 min)
- `FLUSH_INTERVAL` - interval flush ke HDFS (default: 120s = 2 min)
- `HDFS_URI` - Hadoop namenode URI

## Dependensi

```bash
pip install kafka-python requests feedparser pyspark
```

Atau gunakan venv yang sudah ada:
```bash
source .venv/bin/activate
```

## Docker Requirements

Pastikan sudah running:
- **Kafka**: `localhost:9092`
- **Hadoop Namenode**: `localhost:8020`, container `hadoop-namenode`
- **Spark**: dengan Hadoop configuration

## Output & Results

### HDFS Paths
- Raw data: `/data/weather/api/`, `/data/weather/rss/`
- Medallion bronze: `/lakehouse/bronze/`
- Medallion silver: `/lakehouse/silver/`
- Medallion gold: `/lakehouse/gold/`
- Analysis results: `/data/weather/hasil/`

### JSON Report
```
dashboard/data/spark_results.json
```
Contains:
- Metadata (event counts, paths)
- Narasi (interpretasi untuk setiap analisis)
- Data: suhu_per_kota, kondisi_ekstrem, tren_jam

### Web Dashboard
```bash
cd dashboard && python app.py
# Buka: http://localhost:5000
```

## Tips

1. **Pertama kali**: Producer perlu beberapa menit untuk accumulate data
2. **Monitor Kafka**: 
   ```bash
   docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
   ```
3. **Check HDFS**:
   ```bash
   docker exec hadoop-namenode hdfs dfs -ls /data/weather/api/
   ```
4. **Spark UI**: Buka `http://localhost:4040` saat Spark job running
5. **Kill all producers**: `pkill -f producer_api.py`

## Troubleshooting

### Kafka connection error
- Pastikan Kafka running: `docker ps | grep kafka`
- Check broker: `docker logs kafka`

### HDFS permission error
- Set env var: `export HADOOP_USER_NAME=hadoop`

### Spark can't read Delta files
- Pastikan `delta-core` installed di Spark
- Check config di `medallion/*.py`

---

**Created**: 2026-06-03  
**Format**: WeatherPulse Medallion Architecture + Delta Lake
