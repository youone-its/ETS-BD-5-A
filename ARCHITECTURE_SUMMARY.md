# 🏗️ WeatherPulse Data Architecture Summary

## Project Structure Overview

```
ETS-BD-5-A/
├── 📊 MEDALLION ARCHITECTURE (Data Transformation Pipeline)
│   ├── medallion/
│   │   ├── 01_bronze.py           → Raw data ingestion dari Kafka
│   │   ├── 02_silver.py           → Data cleaning & standardization
│   │   ├── 03_gold.py             → Business analytics & aggregations ⭐
│   │   ├── 04_time_travel.py      → Delta Lake time-travel queries
│   │   ├── config.py              → Configuration (KOTA, RSS URLs, HDFS paths)
│   │   ├── spark_analysis.py      → Additional Spark analysis
│   │   └── README_lakehouse.md    → Medallion documentation
│   │
│   └── 🏛️ DELTA LAKE STORAGE: /lakehouse/
│       ├── bronze/     → Raw data tables
│       ├── silver/     → Cleaned data tables
│       └── gold/       → Analytics tables ⭐
│           ├── weather_analytics
│           ├── weather_extremes
│           ├── news_by_source
│           ├── recent_news
│           └── weather_news_correlation
│
├── 📡 KAFKA STREAMING PIPELINE
│   ├── kafka/
│   │   ├── producer_api.py        → Push Open-Meteo API data to Kafka
│   │   ├── producer_rss.py        → Push RSS feed data to Kafka
│   │   ├── consumer_to_hdfs.py    → Consume Kafka → HDFS (Bronze input)
│   │   └── check_kafka.sh         → Kafka health check
│   │
│   ├── producer_api.py            → (Root) API producer
│   ├── producer_rss.py            → (Root) RSS producer
│   ├── consumer_to_hdfs.py        → (Root) HDFS consumer
│   └── config.py                  → Shared configuration
│
├── 🎨 DASHBOARD APPLICATION ⭐
│   ├── dashboard/
│   │   ├── app.py                 → Flask backend + Spark integration
│   │   ├── templates/
│   │   │   └── index.html        → Frontend + Gold layer UI (updated)
│   │   ├── static/
│   │   │   └── style.css         → Styling + Gold layer CSS (updated)
│   │   ├── data/
│   │   │   └── spark_results.json → (Optional) Cached results
│   │   ├── requirements.txt       → Python dependencies
│   │   └── Dockerfile            → Container image
│   │
│   ├── activate_dashboard.sh      → Dashboard startup script
│   └── run_pipeline.py            → Full pipeline orchestration
│
├── 🐳 DOCKER ORCHESTRATION
│   ├── docker-compose.yml         → Full stack (Kafka, Hadoop, Spark, Dashboard)
│   ├── docker-compose-simple.yml  → Simplified version
│   ├── Dockerfile.pipeline        → Pipeline container
│   ├── hadoop/                    → HDFS setup & configuration
│   └── kafka/                     → Kafka setup & configuration
│
├── 📚 DOCUMENTATION
│   ├── README.md                  → Main project overview
│   ├── README_QUICKSTART.md       → Quick start guide
│   ├── PIPELINE.md                → Pipeline workflow
│   ├── MEDALLION_INTEGRATION.md   → Medallion ↔ Dashboard integration ⭐
│   ├── ARCHITECTURE_SUMMARY.md    → This file
│   └── medallion/README_lakehouse.md → Medallion specific docs
│
└── 🔧 UTILITIES & TESTING
    ├── spark_analysis.py          → Standalone Spark analysis
    ├── analysis.py                → Data analysis utilities
    ├── main.ipynb                 → Jupyter notebook exploration
    └── assets/                    → Screenshots & images
```

---

## Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│  Open-Meteo API (Weather)  │  RSS Feeds (News)  │  CSV/Manual   │
└────────────┬─────────────────────────┬──────────────────────────┘
             │                         │
             │         ┌───────────────┘
             │         │
             ▼         ▼
┌──────────────────────────────────────────────────────────────────┐
│  KAFKA PRODUCERS (Real-time Ingestion)                           │
│  producer_api.py (10 min interval)  │  producer_rss.py (5 min)   │
└────────────┬──────────────────────────────────────────┬──────────┘
             │                                         │
    ┌────────▼────────────────────────────────────────▼────────────┐
    │         KAFKA TOPICS (Real-time Streaming)                   │
    │  Topic: weather-api (sensor readings)                        │
    │  Topic: weather-rss  (news articles)                         │
    └─┬─────────────────────────────────────────────────────────┬──┘
      │                                                           │
      │ consumer_to_hdfs.py                    │ dashboard/app.py
      │                                        │
      ▼                                        ▼
┌──────────────────────────┐          ┌──────────────────────┐
│  HDFS RAW DATA STORAGE   │          │  DASHBOARD (Kafka)   │
│  (/data/weather/api)     │          │  Real-time View      │
│  (/data/weather/rss)     │          │  - current_weather   │
│                          │          │  - extreme_weather   │
└───────────┬──────────────┘          │  - latest_news       │
            │                         └──────────────────────┘
            │ Medallion Pipeline
            │
    ┌───────▼────────────────────────────────────────────────┐
    │      MEDALLION ARCHITECTURE (Spark + Delta Lake)        │
    │                                                         │
    │  ┌─────────────────────────────────────────────────┐  │
    │  │ 🔴 BRONZE LAYER (Raw Data)                      │  │
    │  │ - No transformations                            │  │
    │  │ - Tables: weather_api_raw, weather_rss_raw      │  │
    │  │ - Location: /lakehouse/bronze/                  │  │
    │  └──────────────────┬────────────────────────────┬─┘  │
    │                     │                            │     │
    │                 01_bronze.py              02_silver.py │
    │                     │                            │     │
    │  ┌─────────────────▼────────────────────────────▼──┐  │
    │  │ 🟡 SILVER LAYER (Cleaned Data)                │  │
    │  │ - Deduplication                              │  │
    │  │ - Null filtering                             │  │
    │  │ - Type casting & standardization             │  │
    │  │ - Tables: weather_api, weather_rss           │  │
    │  │ - Location: /lakehouse/silver/               │  │
    │  └──────────────────┬─────────────────────────┬──┘  │
    │                     │                         │      │
    │                 03_gold.py                    │      │
    │                     │                         │      │
    │  ┌─────────────────▼─────────────────────────▼──┐  │
    │  │ 🥇 GOLD LAYER (Business Analytics) ⭐       │  │
    │  │                                            │  │
    │  │ WEATHER TABLES:                            │  │
    │  │ ├─ weather_analytics (time-series)        │  │
    │  │ │  • Temp trends, moving averages        │  │
    │  │ │  • Humidity & wind per kota             │  │
    │  │ │                                         │  │
    │  │ └─ weather_extremes (summary stats)       │  │
    │  │    • Max/min/avg temperature             │  │
    │  │    • Temperature range per kota           │  │
    │  │                                           │  │
    │  │ NEWS TABLES:                              │  │
    │  │ ├─ news_by_source (distribution)         │  │
    │  │ │  • Article count per source             │  │
    │  │ │  • Latest article timestamp            │  │
    │  │ │                                         │  │
    │  │ └─ recent_news (ranked articles)         │  │
    │  │    • Top 20 recent articles               │  │
    │  │                                           │  │
    │  │ CORRELATION TABLE:                        │  │
    │  │ └─ weather_news_correlation              │  │
    │  │    • Weather events vs news volume        │  │
    │  │    • Hourly aggregation per kota          │  │
    │  │                                           │  │
    │  │ Location: /lakehouse/gold/                │  │
    │  └──────────────────┬──────────────────────┬──┘  │
    └─────────────────────┼──────────────────────┼─────┘
                          │                      │
                      04_time_travel.py          │
                          │                      │
        ┌─────────────────▼──────────────────────▼──────┐
        │  TIME-TRAVEL QUERIES & VERSION CONTROL        │
        │  - Query data at specific timestamps          │
        │  - Table history & lineage                    │
        │  - Data audit trail                           │
        └──────────────────┬──────────────────────┬─────┘
                           │                      │
            ┌──────────────▼──┐      ┌──────────▼──────────┐
            │  Spark Analysis │      │  Dashboard API      │
            │  (Batch)        │      │  (Streaming)        │
            └────────────────┘      └──────────┬───────────┘
                                               │
                                    ┌──────────▼──────────┐
                                    │  DASHBOARD ENDPOINTS│
                                    │                     │
                                    │ Real-time (Kafka):  │
                                    │ /api/current_weather│
                                    │ /api/extreme_weather│
                                    │ /api/latest_news    │
                                    │                     │
                                    │ Analytics (Gold): ⭐│
                                    │ /api/gold/weather_* │
                                    │ /api/gold/news_*    │
                                    │ /api/gold/correlation
                                    └──────────┬──────────┘
                                               │
                                    ┌──────────▼──────────┐
                                    │  FRONTEND (Browser) │
                                    │  Visualization UI   │
                                    │  Real-time Tables   │
                                    │  Analytics Charts   │
                                    └─────────────────────┘
```

---

## Key Components

### 1. **Data Ingestion (Kafka Producers)**

| File | Purpose | Interval |
|------|---------|----------|
| `kafka/producer_api.py` | Fetch Open-Meteo weather API | 10 minutes |
| `kafka/producer_rss.py` | Parse RSS feeds (Antara, Mongabay) | 5 minutes |
| `kafka/consumer_to_hdfs.py` | Write Kafka topics to HDFS | Continuous |

### 2. **Medallion Transformation Pipeline**

| Layer | File | Input | Output | Purpose |
|-------|------|-------|--------|---------|
| **Bronze** | `01_bronze.py` | HDFS (Kafka topics) | `/lakehouse/bronze/` | Raw data ingestion |
| **Silver** | `02_silver.py` | Bronze tables | `/lakehouse/silver/` | Data cleaning, dedup, validation |
| **Gold** | `03_gold.py` | Silver tables | `/lakehouse/gold/` | Business analytics, aggregations |
| **Time Travel** | `04_time_travel.py` | Gold tables | Queries | Query at specific timestamps |

### 3. **Dashboard Integration**

| Component | File | Function |
|-----------|------|----------|
| **Backend** | `dashboard/app.py` | Flask server + Spark session (NEW: Gold layer integration) |
| **Frontend** | `dashboard/templates/index.html` | Dashboard UI (NEW: 4 Gold layer sections) |
| **Styling** | `dashboard/static/style.css` | CSS (NEW: Gold layer styling) |

### 4. **Configuration**

| File | Scope | Contains |
|------|-------|----------|
| `medallion/config.py` | Medallion pipeline | KOTA, RSS URLs, BOOTSTRAP_SERVERS, HDFS paths |
| `config.py` | Root/Kafka | Shared configuration |
| `dashboard/requirements.txt` | Dashboard | Flask, PySpark, Delta, Kafka dependencies |

---

## Gold Layer Tables Deep Dive

### **weather_analytics** (Time-Series)
```python
Schema:
- kode_kota: STRING           # City code (JKT, SBY, etc)
- nama_kota: STRING           # City name
- temp_current: DOUBLE        # Current temperature
- temp_change_1h: DOUBLE      # Temp change in last hour
- temp_moving_avg_3h: DOUBLE  # 3-hour moving average
- humidity: INT               # Current humidity %
- wind_speed: DOUBLE          # Current wind speed km/h
- reading_time: TIMESTAMP     # Measurement timestamp

Window Functions:
- LAG() for temp_change calculation
- AVG() with rangeBetween for 3-hour moving average
```

**Use Cases**:
- Detect rapid temperature changes (alerts)
- Identify weather trends
- Compare patterns across cities

---

### **weather_extremes** (Summary Statistics)
```python
Schema:
- kode_kota: STRING
- nama_kota: STRING
- max_temp: DOUBLE            # Daily/period max
- min_temp: DOUBLE            # Daily/period min
- avg_temp: DOUBLE            # Period average
- temp_range: DOUBLE          # max - min
- max_humidity: INT           # Period max humidity
- max_wind: DOUBLE            # Period max wind speed
- reading_count: LONG         # Number of readings analyzed

GROUP BY kota
```

**Use Cases**:
- City health summary
- Extreme weather detection
- Climate comparison

---

### **news_by_source** (News Distribution)
```python
Schema:
- sumber: STRING              # News source
- article_count: LONG         # Total articles from source
- latest_article: TIMESTAMP   # Most recent article time

GROUP BY sumber
ORDER BY article_count DESC
```

**Use Cases**:
- Source reliability
- Coverage analysis
- Editorial balance

---

### **recent_news** (Ranked Articles)
```python
Schema:
- news_rank: INT              # Ranking 1-20
- judul: STRING               # Article title
- sumber: STRING              # News source
- waktu_terbit: TIMESTAMP     # Publication time
- ringkasan: STRING           # Article summary

FILTER: rank <= 20
ORDER BY waktu_terbit DESC
```

**Use Cases**:
- Recent events feed
- Trending topics
- News aggregation

---

### **weather_news_correlation** (Hourly Aggregation)
```python
Schema:
- kode_kota: STRING
- nama_kota: STRING
- hour: STRUCT (start TIMESTAMP, end TIMESTAMP)
- avg_temp: DOUBLE            # Hourly average temp
- max_humidity: INT           # Hourly max humidity
- max_wind: DOUBLE            # Hourly max wind
- news_count: INT             # News articles in that hour

WINDOW BY: window(column, "1 hour")
```

**Use Cases**:
- **Correlation Analysis**: Does extreme weather trigger news?
- Event Detection: When do stories spike?
- Predictive Insights: Can we predict news volume from weather?

---

## Technologies Used

| Component | Technology | Version |
|-----------|-----------|---------|
| **Data Warehouse** | Apache Spark SQL | 3.x |
| **Storage Format** | Delta Lake | 2.x |
| **Streaming** | Apache Kafka | 3.x |
| **File System** | HDFS (Hadoop) | 3.x |
| **Web Framework** | Flask | 2.x |
| **Frontend** | HTML5 + JavaScript | ES6+ |
| **Charting** | Chart.js | 3.x |
| **Containerization** | Docker & Docker Compose | Latest |

---

## Performance Considerations

### Bronze Layer
- **Volume**: Millions of weather readings + thousands of articles daily
- **Frequency**: Updates every 5-10 minutes
- **Storage**: HDFS append-only (immutable)

### Silver Layer
- **Deduplication**: Removes exact duplicates using Window functions
- **Cleaning**: Filters null values, standardizes types
- **Size**: ~10-15% smaller than Bronze (after dedup)

### Gold Layer
- **Aggregation**: Window functions for time-series analysis
- **Computation**: Done once, used many times
- **Size**: ~1% of Silver (highly aggregated)
- **Query Speed**: Fast (pre-computed analytics)

### Dashboard
- **Caching**: Golden tables are stable, can be cached
- **Real-time**: Kafka stream for live updates
- **Auto-refresh**: Every 10 seconds (configurable)

---

## File Modification Summary (Latest Changes)

### ✅ **dashboard/app.py**
```python
# NEW
- Import PySpark & Delta Lake
- Initialize spark_session for Gold layer access
- Add 5 new endpoints for Gold layer:
  * /api/gold/weather_analytics
  * /api/gold/weather_extremes
  * /api/gold/news_analytics
  * /api/gold/recent_news
  * /api/gold/weather_news_correlation
```

### ✅ **dashboard/templates/index.html**
```html
<!-- NEW Section: Gold Layer Analytics -->
- 4 new subsections for analytics visualization
- Weather time-series table with moving averages
- Weather extremes summary
- News source distribution
- Weather-news correlation dual-axis chart
- JavaScript functions to fetch & update Gold layer data
```

### ✅ **dashboard/static/style.css**
```css
/* NEW */
- .panel-gold-analytics styling with gold gradient
- .correlation-table & .correlation-container styles
- Temperature indicator colors (temp-up, temp-down)
- Gold layer animation & hover effects
```

### ✅ **MEDALLION_INTEGRATION.md** (NEW)
Complete documentation on:
- Architecture overview
- Gold layer table schemas
- API endpoint specifications
- Dashboard integration details
- Running instructions
- Troubleshooting guide

---

## Quick Start

```bash
# 1. Start full Docker stack
docker-compose up -d

# 2. Check services
docker ps

# 3. Run medallion pipeline (once data is available)
docker exec -it spark_container python medallion/03_gold.py

# 4. Access dashboard
open http://localhost:5000

# 5. Verify Gold layer endpoints
curl http://localhost:5000/api/gold/weather_analytics | jq
```

---

## Next Phase: Future Improvements

🔮 **Phase 2 Enhancements**:
- [ ] Real-time WebSocket updates for live correlation
- [ ] Machine learning models for weather forecasting
- [ ] Anomaly detection in news volume
- [ ] Data quality dashboards
- [ ] Cost optimization (Iceberg format consideration)
- [ ] Advanced alerting (PagerDuty/Slack integration)
- [ ] Historical analysis (compare year-over-year trends)

---

**Documentation Version**: 1.0  
**Last Updated**: 2026-06-04  
**Maintained By**: WeatherPulse Team
