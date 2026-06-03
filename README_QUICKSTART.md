# WeatherPulse Pipeline - Quick Start Guide

## 🎯 What You Have

A complete **Big Data Pipeline** dengan:
- ✅ Producer APIs (Weather & News)
- ✅ Kafka/HDFS Consumer
- ✅ Delta Lake Medallion Architecture (Bronze → Silver → Gold)
- ✅ Spark Analytics
- ✅ Dashboard

## 🚀 Quickest Way to Run (Recommended)

### Option 1: Docker (Semi-working, best effort)

```bash
docker-compose -f docker-compose-simple.yml up --build
```

**Note**: Known issues:
- Kafka ZooKeeper requires additional configuration
- Spark image needs Python3 symlink fix
- Better for quick integration test of Spark/Hadoop layers

### Option 2: Local Python (BEST for testing)

```bash
# 1. Fetch sample weather data from API
python producer_api.py

# 2. On different terminal - process with Spark
python medallion/01_bronze.py
python medallion/02_silver.py
python medallion/03_gold.py
python medallion/spark_analysis.py

# 3. Check results
cat dashboard/data/spark_results.json | jq .
```

This generates sample data without needing Kafka/Hadoop.

## 📁 Directory Structure

```
.
├── config.py                    # Shared configuration
├── producer_api.py              # Fetch weather (6 cities)
├── producer_rss.py              # Fetch news (RSS feeds)
├── consumer_to_hdfs.py          # Kafka → storage
├── spark_analysis.py            # Final analysis
├── run_pipeline.py              # Orchestration

medallion/
├── 01_bronze.py                 # Raw → Delta
├── 02_silver.py                 # Clean → Delta
├── 03_gold.py                   # Analyze → Delta
├── spark_analysis.py            # Results

dashboard/
├── app.py                       # Flask web UI
├── data/spark_results.json      # Analysis output

docker-compose.yml              # Full stack (with Kafka)
docker-compose-simple.yml       # Spark/Hadoop only
```

## ✨ Features Implemented

| Feature | Status | Details |
|---------|--------|---------|
| **Data Ingestion** | ✅ Complete | 6 Indonesian cities weather API |
| **Delta Lake** | ✅ Complete | ACID transactions, time travel |
| **Medallion Architecture** | ✅ Complete | Bronze → Silver → Gold |
| **Spark Analytics** | ✅ Complete | 3 analyses: stats, extremes, trends |
| **Dashboard** | ✅ Ready | Flask web app (needs data first) |
| **Docker Compose** | ⚠️ Partial | Spark/Hadoop work; Kafka needs fix |
| **Retry Logic** | ✅ Complete | Exponential backoff in producers |

##  Test Data Sample

When you run `python producer_api.py`:

```
✅ JKT | Jakarta    | 28.5°C | 4.2 km/h  | 77% humidity
✅ SBY | Surabaya   | 29.0°C | 11.6 km/h | 68% humidity
✅ SMG | Semarang   | 27.6°C | 2.4 km/h  | 78% humidity
✅ MDN | Medan      | 30.0°C | 2.8 km/h  | 60% humidity
✅ MKS | Makassar   | 27.9°C | 3.1 km/h  | 82% humidity
✅ DPS | Denpasar   | 25.9°C | 6.6 km/h  | 83% humidity
```

All real data from Open-Meteo API ✅

## 🧪 What Pipeline Does

### Input
- Weather API: 6 cities in Indonesia, real-time data
- RSS News: 2 weather news sources

### Processing (Medallion Architecture)
1. **Bronze**: Raw ingest (JSON → Parquet/Delta)
2. **Silver**: Clean, deduplicate, type-cast
3. **Gold**: Aggregate, analyze, create metrics

### Output
```json
{
  "metadata": {
    "total_events": 216,
    "timestamp": "2026-06-03T23:00:00Z"
  },
  "analyses": {
    "suhu_per_kota": [
      {"nama_kota": "Surabaya", "suhu_avg": 31.58, "suhu_tertinggi": 32.3}
    ],
    "kondisi_ekstrem": [...],
    "tren_jam": [...]
  }
}
```

## 🔧 Requirements

```bash
# Core
pip install pyspark==3.5.0 delta-spark==3.1.0 pandas requests feedparser

# Optional: Dashboard
pip install flask

# Docker (optional)
docker-compose
```

Or use existing venv:
```bash
source .venv/bin/activate
```

## 📊 Next Steps

1. **Test Medallion**: `python medallion/01_bronze.py`
2. **Generate Data**: `python producer_api.py`
3. **View Results**: `cat dashboard/data/spark_results.json | jq`
4. **Run Dashboard**: `python dashboard/app.py` → http://localhost:5000

## 🐛 Known Issues

- **Kafka ZooKeeper**: Connection timeout (fixable with proper image)
- **Docker Python**: Spark image needs `python3` symlink

## ✅ What Works 100%

- ✅ Producer API fetches real weather data
- ✅ Spark Medallion layers process data correctly
- ✅ Delta Lake formatting with ACID transactions
- ✅ Analytics generate correct JSON output
- ✅ Dashboard can read and display results

## 📝 Architecture Benefits

1. **Delta Lake**: Time travel, ACID, rollback capability
2. **Medallion**: Separated concerns (raw → clean → analytics)
3. **Modular**: Each component runs independently
4. **Scalable**: Easily move to full Hadoop/Spark cluster

---

**Created**: 2026-06-03  
**Status**: Production-ready for data processing, Docker Kafka layer needs minor config  
**Next**: Run `python medallion/01_bronze.py` to test
