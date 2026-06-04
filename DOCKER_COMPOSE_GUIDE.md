# 🐳 Docker Compose Full Stack Guide

## Complete WeatherPulse System dengan Docker Compose

Dashboard sekarang fully terintegrasi dalam docker-compose stack! Artinya, ketika Anda menjalankan `docker-compose up`, semua services akan berjalan otomatis:

- ✅ Kafka (message broker)
- ✅ HDFS (Hadoop file system)
- ✅ Spark & Medallion pipeline (Bronze, Silver, Gold)
- ✅ Producers (API & RSS)
- ✅ Consumer (HDFS)
- ✅ **Dashboard (Flask + Spark integration)** ⭐

---

## 🚀 Quick Start

### Prerequisites
```bash
# Install Docker & Docker Compose
# - Docker Desktop (Mac/Windows)
# - docker-ce + docker-compose (Linux)

# Verify installation
docker --version
docker-compose --version
```

### 1. Jalankan Full Stack

```bash
# Clone/navigate ke project
cd /path/to/ETS-BD-5-A

# Start semua services
docker-compose up -d

# Monitor progress
docker-compose logs -f

# Check status
docker-compose ps
```

**Expected output**:
```
CONTAINER ID   IMAGE                      COMMAND                  STATUS
...
xxx            etsbd-kafka                "kafka-server-start"     Up (healthy)
xxx            etsbd-hadoop               "hdfs namenode"          Up (healthy)
xxx            etsbd-spark-medallion      "python 01_bronze.py"    Exited (0)
xxx            etsbd-spark-medallion      "python 02_silver.py"    Exited (0)
xxx            etsbd-spark-medallion      "python 03_gold.py"      Exited (0)
xxx            etsbd-producer-api         "python producer_api"    Up
xxx            etsbd-producer-rss         "python producer_rss"    Up
xxx            etsbd-consumer-hdfs        "python consumer_to_hdfs" Up
xxx            etsbd-dashboard            "python app.py"          Up (healthy) ⭐
```

### 2. Akses Dashboard

Buka browser ke:
```
http://localhost:5000
```

**Dashboard akan menampilkan**:
- ✅ Real-time weather data (dari Kafka)
- ✅ Extreme weather alerts (dari Kafka)
- ✅ Latest news (dari Kafka)
- ✅ **Gold layer analytics** (dari Delta Lake):
  - Time-series weather analysis
  - Weather extremes summary
  - News source distribution
  - Weather-news correlation chart

---

## 📊 Services Overview

### Kafka Broker
```yaml
Container: kafka-broker
Port: 9092
Topics: weather-api, weather-rss
Status: Healthy check via kafka-broker-api-versions
```

### HDFS (Hadoop Distributed File System)
```yaml
Containers: 
  - hadoop-namenode (port 9870 UI)
  - hadoop-datanode
  - hadoop-resourcemanager (port 8088 UI)
  - hadoop-nodemanager (port 8042 UI)
Paths:
  - /data/weather/api (from Kafka)
  - /data/weather/rss (from Kafka)
```

### Medallion Pipeline
```yaml
Sequential execution:
1. medallion-bronze    (01_bronze.py)   → Ingest from HDFS
2. medallion-silver    (02_silver.py)   → Clean & dedup
3. medallion-gold      (03_gold.py)     → Analytics aggregation
4. spark-analysis      (spark_analysis.py) → Additional analysis

Output location:
  - /lakehouse/bronze/
  - /lakehouse/silver/
  - /lakehouse/gold/ ⭐
```

### Data Producers
```yaml
producer-api:
  - Fetches Open-Meteo weather API
  - Publishes to: weather-api topic
  - Interval: 10 minutes

producer-rss:
  - Parses RSS feeds (Antara, Mongabay)
  - Publishes to: weather-rss topic
  - Interval: 5 minutes
```

### Data Consumer
```yaml
consumer-hdfs:
  - Consumes Kafka topics
  - Writes to HDFS (/data/weather/)
  - Input for medallion pipeline
```

### Dashboard (Flask + Spark)
```yaml
Container: etsbd-dashboard
Port: 5000
URL: http://localhost:5000

Services:
- Kafka consumers (real-time data)
- Spark session (Gold layer access)
- Health check: /api/health
```

---

## 📁 Volume Mounts

| Container | Host Path | Container Path | Purpose |
|-----------|-----------|-----------------|---------|
| All Medallion | ./medallion/ | /app | Medallion scripts |
| All | ./medallion/lakehouse_data | /lakehouse | Delta Lake storage ⭐ |
| Dashboard | ./dashboard/data | /app/data | Dashboard data cache |
| Consumer-HDFS | ./data | /data | HDFS data export |

**Important**: Lakehouse folder (`./medallion/lakehouse_data`) adalah **shared volume** yang berisi:
- Delta tables dari medallion pipeline
- Diakses oleh dashboard via Spark
- Persisten di disk

---

## 🔄 Data Flow dalam Docker

```
┌─────────────────────────────────────────────────────┐
│ PRODUCERS (Inside containers)                       │
├─────────────────────────────────────────────────────┤
│ producer-api → weather-api topic (Kafka)           │
│ producer-rss → weather-rss topic (Kafka)           │
└────────────┬────────────────────────────────────────┘
             │
    ┌────────▼────────────────────────────────────────┐
    │ KAFKA BROKER (kafka-broker:9092)                │
    │ Topics: weather-api, weather-rss                │
    └────────┬─────────────────┬──────────────────────┘
             │                 │
     ┌───────▼────────┐  ┌────▼──────────────┐
     │ consumer-hdfs  │  │ dashboard         │
     │ → HDFS /data/  │  │ (real-time view)  │
     └───────┬────────┘  └──────────────────┘
             │
     ┌───────▼──────────────────────────────┐
     │ HDFS Storage (/data/weather/*)       │
     └───────┬──────────────────────────────┘
             │
     ┌───────▼──────────────────────────────┐
     │ MEDALLION PIPELINE (Sequential)      │
     │                                      │
     │ 1️⃣  bronze   (01_bronze.py)        │
     │ 2️⃣  silver   (02_silver.py)        │
     │ 3️⃣  gold     (03_gold.py) ⭐       │
     │ 4️⃣  analysis (spark_analysis.py)   │
     │                                      │
     │ Output: /lakehouse/ volumes          │
     └───────┬──────────────────────────────┘
             │
             ▼
     ┌──────────────────────────────┐
     │ Delta Lake Tables (Gold)     │
     │ /lakehouse/gold/             │
     │                              │
     │ ├─ weather_analytics         │
     │ ├─ weather_extremes          │
     │ ├─ news_by_source            │
     │ ├─ recent_news               │
     │ └─ weather_news_correlation  │
     └───────┬──────────────────────┘
             │
             ▼
     ┌────────────────────────────────────┐
     │ DASHBOARD (Flask + Spark)          │
     │ http://localhost:5000 ⭐           │
     │                                    │
     │ Real-time endpoints:               │
     │ - /api/current_weather (Kafka)     │
     │ - /api/extreme_weather (Kafka)     │
     │ - /api/latest_news (Kafka)         │
     │                                    │
     │ Gold layer endpoints:              │
     │ - /api/gold/weather_analytics      │
     │ - /api/gold/weather_extremes       │
     │ - /api/gold/news_analytics         │
     │ - /api/gold/recent_news            │
     │ - /api/gold/weather_news_correla.. │
     └────────────────────────────────────┘
             │
             ▼
     ┌────────────────────────────────────┐
     │ BROWSER (Visualization)            │
     │ localhost:5000                     │
     └────────────────────────────────────┘
```

---

## 🔍 Common Commands

### Manage Stack

```bash
# Start all services (background)
docker-compose up -d

# View logs
docker-compose logs -f                    # All services
docker-compose logs -f dashboard          # Only dashboard
docker-compose logs -f medallion-gold     # Only gold layer

# Check status
docker-compose ps

# Stop all services
docker-compose stop

# Stop and remove containers
docker-compose down

# Remove volumes (WARNING: loses data)
docker-compose down -v

# Restart specific service
docker-compose restart dashboard
```

### Debug Individual Services

```bash
# Open shell in dashboard container
docker-compose exec dashboard bash

# Inside container - check Spark
python3 -c "from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate())"

# Test Gold layer connection
curl http://localhost:5000/api/gold/weather_analytics | jq

# View Spark logs
docker-compose logs spark

# Check HDFS
docker-compose exec namenode hdfs dfs -ls /data/weather/
```

### Monitor Resources

```bash
# Real-time resource usage
docker stats

# Specific container
docker stats etsbd-dashboard
```

---

## 🧪 Testing Dashboard Integration

### 1. Wait for all services to be healthy

```bash
# Monitor logs until you see:
# ✅ Kafka: "[KafkaServer id=1] started"
# ✅ HDFS: "Namenode started"
# ✅ Medallion: "Gold layer complete!"
# ✅ Dashboard: "✨ Server starting at http://localhost:5000"

docker-compose logs | grep "started\|complete\|Server starting"
```

### 2. Verify dashboard is running

```bash
# Health check
curl http://localhost:5000/api/health | jq

# Expected response:
# {
#   "status": "ok",
#   "cities_with_data": 6,
#   "news_count": 42,
#   "gold_layer_available": true
# }
```

### 3. Test all endpoints

```bash
# Real-time data (Kafka)
curl http://localhost:5000/api/current_weather | jq '.JKT'

# Gold layer analytics
curl http://localhost:5000/api/gold/weather_analytics | jq '.data[0]'
curl http://localhost:5000/api/gold/weather_extremes | jq '.data[0]'
curl http://localhost:5000/api/gold/news_analytics | jq '.data[0]'
curl http://localhost:5000/api/gold/weather_news_correlation | jq '.data[0]'
```

### 4. Open in browser

```
http://localhost:5000
```

**Expected UI sections**:
- 🚨 Extreme Weather Alerts
- 🌡️ Current Weather Status
- 🥇 Gold Layer Analytics:
  - ⏱️ Time-Series Analytics
  - 🌡️ Weather Extremes
  - 📰 News Distribution
  - 🔗 Weather-News Correlation (with chart)
- 📰 Latest News

---

## 🔧 Configuration

### Environment Variables (set in docker-compose.yml)

| Variable | Value | Purpose |
|----------|-------|---------|
| `BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `HDFS_HOST` | `namenode` | HDFS namenode hostname |
| `HDFS_PORT` | `8020` | HDFS namenode port |
| `LAKEHOUSE_PATH` | `/lakehouse` | Delta Lake storage path |
| `PYTHONUNBUFFERED` | `1` | Real-time logs |
| `FLASK_ENV` | `production` | Flask mode |

### Port Mapping

| Service | Host Port | Container Port | URL |
|---------|-----------|------------------|-----|
| **Dashboard** ⭐ | **5000** | **5000** | **http://localhost:5000** |
| Kafka | 9092 | 9092 | localhost:9092 |
| HDFS NameNode | 9870 | 9870 | http://localhost:9870 |
| Yarn ResourceMgr | 8088 | 8088 | http://localhost:8088 |
| NodeManager | 8042 | 8042 | http://localhost:8042 |

---

## 🐛 Troubleshooting

### Dashboard tidak bisa akses Gold layer

```bash
# Check if medallion-gold completed successfully
docker-compose logs medallion-gold | tail -20

# Check if lakehouse volume exists
ls -la ./medallion/lakehouse_data/gold/

# Verify Spark in dashboard
docker-compose exec dashboard python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print("✅ Spark working!")
EOF
```

### Dashboard tidak muncul di port 5000

```bash
# Check if container is running
docker-compose ps | grep dashboard

# Check logs
docker-compose logs dashboard | tail -50

# Try restarting
docker-compose restart dashboard

# Check if port is already in use
lsof -i :5000
```

### Kafka tidak connect

```bash
# Check Kafka health
docker-compose logs kafka | grep "started"

# Test connection from producer
docker-compose exec producer-api kafka-broker-api-versions.sh \
  --bootstrap-server kafka:9092
```

### HDFS not healthy

```bash
# Check namenode logs
docker-compose logs namenode | grep -i "error\|exception"

# Format namenode (last resort - loses data!)
docker-compose exec namenode hdfs namenode -format
```

### Medallion pipeline failed

```bash
# Check each stage
docker-compose logs medallion-bronze | grep -i "error"
docker-compose logs medallion-silver | grep -i "error"
docker-compose logs medallion-gold | grep -i "error"

# Check lakehouse data
docker-compose exec medallion-gold hdfs dfs -ls /lakehouse/gold/
```

---

## 📊 Performance Tips

### Reduce startup time
```bash
# Use simple compose (no spark analysis)
docker-compose -f docker-compose-simple.yml up -d
```

### Monitor resource usage
```bash
# Watch memory/CPU
docker stats --no-stream

# Set resource limits in compose (optional)
# services:
#   dashboard:
#     deploy:
#       resources:
#         limits:
#           cpus: '2'
#           memory: 2G
```

### Optimize for development
- Keep medallion pipeline disabled until needed
- Use `-f docker-compose-simple.yml` for faster startup
- Set `PYTHONUNBUFFERED=1` for live logs

---

## 🎯 Next Steps

1. ✅ **Run full stack**: `docker-compose up -d`
2. ✅ **Wait for startup**: `docker-compose logs -f` (5-10 minutes)
3. ✅ **Verify health**: `curl http://localhost:5000/api/health`
4. ✅ **Open dashboard**: http://localhost:5000
5. ✅ **Monitor real-time data**: Watch tables auto-refresh
6. ✅ **Check analytics**: View Gold layer charts & tables

---

## 📚 Related Documentation

- **MEDALLION_INTEGRATION.md** - Integration architecture
- **ENDPOINTS_REFERENCE.md** - API endpoint details
- **ARCHITECTURE_SUMMARY.md** - System overview
- **README_QUICKSTART.md** - Quick start guide

---

## 🆘 Getting Help

```bash
# Comprehensive logs
docker-compose logs > debug.log

# Individual service logs
docker-compose logs dashboard > dashboard.log
docker-compose logs medallion-gold > medallion.log

# System info
docker system df
docker ps -a
docker images
```

Share these logs if you need help troubleshooting!

---

**Last Updated**: 2026-06-04  
**Dashboard Port**: 5000  
**Status**: ✅ Fully integrated with docker-compose
