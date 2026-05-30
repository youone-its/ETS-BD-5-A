# 🏛️ Medallion Architecture - Data Lakehouse

## Overview
Implementasi Medallion Architecture (Bronze → Silver → Gold) menggunakan Delta Lake untuk ETS Big Data project.
Data mengalir dari Kafka producers (weather API & RSS) melalui HDFS, kemudian di-transform menjadi analytics tables.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                  │
│  ┌──────────────┐              ┌──────────────┐                 │
│  │ Weather API  │              │ Weather RSS  │                 │
│  │  (producer)  │              │   (feed)     │                 │
│  └──────────────┘              └──────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
         ↓                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA TOPICS                                  │
│  ┌──────────────┐              ┌──────────────┐                 │
│  │ weather-api  │              │ weather-rss  │                 │
│  └──────────────┘              └──────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
         ↓                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                    HDFS STORAGE                                  │
│  ┌──────────────┐              ┌──────────────┐                 │
│  │  /data/*     │              │  /data/*     │                 │
│  │   (JSON)     │              │   (JSON)     │                 │
│  └──────────────┘              └──────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
         ↓                               ↓
    ┌────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────┐
│         🥉 BRONZE LAYER (01_bronze.py)                          │
│  - Ingest raw JSON dari HDFS                                    │
│  - Add _ingested_at timestamp & _source kolom                   │
│  - Basic deduplication (message level)                          │
│                                                                 │
│  📁 /lakehouse/bronze/                                          │
│  ├── weather_api/  (Delta format)                               │
│  └── weather_rss/  (Delta format)                               │
└─────────────────────────────────────────────────────────────────┘
         ↓ (02_silver.py)
┌─────────────────────────────────────────────────────────────────┐
│         🥈 SILVER LAYER (02_silver.py)                          │
│  - Data cleaning & validation                                   │
│  - NULL filtering, type casting                                 │
│  - Deduplication by domain keys                                 │
│  - Standardisasi kolom & format                                 │
│                                                                 │
│  📁 /lakehouse/silver/                                          │
│  ├── weather_api/  (Cleaned & enriched)                         │
│  └── weather_rss/  (Cleaned & enriched)                         │
└─────────────────────────────────────────────────────────────────┘
         ↓ (03_gold.py)
┌─────────────────────────────────────────────────────────────────┐
│         🥇 GOLD LAYER (03_gold.py)                              │
│  - Business-ready analytics tables                              │
│  - Aggregations & window functions                              │
│  - Cross-join weather + news correlation                        │
│                                                                 │
│  📁 /lakehouse/gold/                                            │
│  ├── weather_analytics/        (Time-series per kota)           │
│  ├── weather_extremes/         (Summary stats)                  │
│  ├── news_by_source/           (News distribution)              │
│  ├── recent_news/              (Top 20 articles)                │
│  └── weather_news_correlation/ (Weather + News join)            │
└─────────────────────────────────────────────────────────────────┘
         ↓ (04_time_travel.py)
┌─────────────────────────────────────────────────────────────────┐
│         ⏰ TIME TRAVEL DEMO                                      │
│  - Update Gold table (+5°C correction)                          │
│  - Query versi lama (before update)                             │
│  - Query versi baru (after update)                              │
│  - Compare old vs new dengan Delta Lake                         │
└─────────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────────┐
│    DASHBOARD (Flask - reads from Gold Delta tables)             │
│  - Real-time analytics dari Gold layer                          │
│  - Replaces old spark_results.json approach                     │
└─────────────────────────────────────────────────────────────────┘
```

## Layer Details

### 🥉 BRONZE LAYER (Raw Data Ingestion)
**File:** `01_bronze.py`

**Input:** JSON files dari Kafka (via HDFS)
```json
{
  "kode_kota": "JKT",
  "nama_kota": "Jakarta",
  "temperature": 28.5,
  "humidity": 75,
  "wind_speed": 10,
  "weather_code": 0,
  "timestamp": "2026-05-06 14:30:45"
}
```

**Transformasi:**
1. ✅ Read raw JSON dari HDFS paths
2. ✅ Add metadata columns:
   - `_ingested_at`: Timestamp ingestion
   - `_source`: "weather-api" atau "weather-rss"
3. ✅ Basic deduplication (Kafka message level)

**Output:** Delta Lake tables dengan lineage tracking

---

### 🥈 SILVER LAYER (Cleaned & Standardized)
**File:** `02_silver.py`

**Transformasi (3 kategori):**

#### 1️⃣ NULL Handling & Filtering
- Filter rows dengan `kode_kota IS NOT NULL`
- Filter readings dengan valid temperature/humidity/wind
- Hapus duplicate articles

#### 2️⃣ Type Casting & Validation
- Cast `humidity` ke INTEGER
- Cast `timestamp` ke TIMESTAMP
- Round temperature/wind ke 2 decimal
- Standardisasi `kode_kota` ke UPPERCASE

#### 3️⃣ Deduplication
- Weather: Keep latest reading per `(kode_kota, timestamp)`
- News: Keep latest per `(judul, sumber)` (avoid duplicate articles)
- Gunakan Window function dengan ROW_NUMBER()

**Output:** Clean, deduplicated Delta tables siap untuk analytics

---

### 🥇 GOLD LAYER (Analytics & Business Logic)
**File:** `03_gold.py`

**5 Analytics Tables:**

#### 1. `weather_analytics` (Time-Series per Kota)
```
kode_kota | nama_kota | temp_current | temp_change_1h | temp_moving_avg_3h | ...
```
- Window function: LAG untuk detect trend
- Moving average: 3-hour rolling avg temperature
- Reproduces old ETS Spark analysis

#### 2. `weather_extremes` (Summary Statistics)
```
kode_kota | max_temp | min_temp | avg_temp | max_humidity | max_wind | ...
```
- Aggregate per kota
- Detect extreme conditions (temp > 35°C, wind > 40 km/h)

#### 3. `news_by_source` (News Distribution)
```
sumber | article_count | latest_article | ...
```
- Count berita per RSS source
- Track latest article timestamp

#### 4. `recent_news` (Top 20 Articles)
```
news_rank | judul | sumber | waktu_terbit | ringkasan | ...
```
- Rank by recency (RANK OVER ORDER BY waktu_terbit DESC)
- Keep top 20 latest articles

#### 5. `weather_news_correlation` (Cross-Join Analysis)
```
kode_kota | hour_window | avg_temp | max_humidity | news_count | ...
```
- Window function: Group weather by 1-hour buckets
- Join dengan news count dalam same hour
- Analisis correlation antara cuaca ekstrem & news frequency

---

### ⏰ TIME TRAVEL & VERSION CONTROL
**File:** `04_time_travel.py`

Demonstrasi Delta Lake capabilities:

1. **Read Initial Version** (v0)
   ```python
   weather_analytics.limit(5).show()  # Original data
   ```

2. **Perform Update**
   ```python
   deltaTable.update(
       condition="temp_current IS NOT NULL",
       set={"temp_current": col("temp_current") + 5.0}
   )  # Simulasi sensor correction
   ```

3. **Query New Version** (v1)
   ```python
   spark.read.format("delta").load(path)  # Latest data
   ```

4. **Time Travel Query** (back to v0)
   ```python
   spark.read.format("delta") \
       .option("versionAsOf", 0) \
       .load(path)  # Query versi lama
   ```

5. **Comparison: Old vs New**
   - Side-by-side comparison
   - Show temperature differences
   - Print summary statistics

6. **Delta Log History**
   ```
   version | timestamp | operation | operationParameters
   0       | ...       | WRITE     | ...
   1       | ...       | UPDATE    | ...
   ```

---

## Running the Pipeline

### Prerequisites
- Docker Compose dengan services: kafka, namenode, dashboard
- Spark container running

### Execution Order

```bash
# 1. Start all services (kafka, hadoop, dashboard, spark)
docker-compose up -d

# 2. Run medallion pipeline in sequence
docker exec spark-medallion python 01_bronze.py
docker exec spark-medallion python 02_silver.py
docker exec spark-medallion python 03_gold.py
docker exec spark-medallion python 04_time_travel.py
```

### Directory Structure

```
medallion/
├── Dockerfile
├── requirements.txt
├── 01_bronze.py          # Ingestion dari HDFS JSON → Bronze Delta
├── 02_silver.py          # Cleaning & validation → Silver Delta
├── 03_gold.py            # Analytics & aggregations → Gold Delta
├── 04_time_travel.py     # Version control demo
├── README_lakehouse.md   # This file
└── lakehouse_data/       # Persisted Delta Lake storage
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## Data Quality & Cleaning Justification

### Bronze → Silver Transformations

| Issue | Bronze State | Silver Cleaning |
|-------|---|---|
| **NULL Temperature** | Accepts | ❌ Filtered out |
| **Type Safety** | String numbers | ✅ Cast to proper types |
| **Duplicates** | Raw Kafka messages | ✅ Dedup by (city, timestamp) |
| **Time Inconsistency** | Multiple formats | ✅ Standardized to TIMESTAMP |
| **Case Sensitivity** | Mixed case kota codes | ✅ UPPERCASE normalization |
| **NULL Articles** | Accepts empty fields | ❌ Filtered; ringkasan defaulted to "" |

### Deduplication Logic

- **Weather**: Keep LATEST per city (by timestamp) → ensures single source of truth
- **News**: Keep LATEST per (title, source) → prevents showing duplicate articles
- **Method**: Window function `ROW_NUMBER()` over partition/order, keep row_num = 1

---

## Analytics Value

### Gold Tables Enable:
1. **Real-Time Dashboards** (Dashboard reads from Gold, not static JSON)
2. **Time-Series Analysis** (Track trends with moving averages & lags)
3. **Extreme Weather Alerts** (Pre-computed extreme conditions)
4. **Content Analytics** (News distribution & correlation with weather)
5. **Data Versioning** (Complete audit trail with Delta Lake)

### Comparison: Gold vs Old ETS Analysis
| Aspect | Old Spark (spark_results.json) | Gold Delta Layer |
|--------|---|---|
| **Lineage** | Static JSON dump | ✅ Full version history |
| **Freshness** | One-time calculation | ✅ Incremental updates |
| **Correlation** | API only | ✅ Weather + News joined |
| **Time Travel** | ❌ Not possible | ✅ Query any version |
| **Scalability** | File-based | ✅ Delta Lake optimized |

---

## Bonus: Dashboard Integration

The Flask dashboard can be updated to read from Gold Delta tables:

```python
# OLD: Read from static JSON
with open("data/spark_results.json") as f:
    results = json.load(f)

# NEW: Read from Delta Gold layer
df_weather = spark.read.format("delta").load("/lakehouse/gold/weather_analytics")
df_extremes = spark.read.format("delta").load("/lakehouse/gold/weather_extremes")
results = {"weather": df_weather.toJSON(), "extremes": df_extremes.toJSON()}
```

This ensures dashboard always shows fresh analytics instead of stale pre-computed results.

---

## Summary

**Medallion Architecture Benefits:**
- 🥉 **Bronze**: Immutable raw data with lineage
- 🥈 **Silver**: Cleaned, trusted data layer
- 🥇 **Gold**: Ready-to-consume analytics tables
- ⏰ **Time Travel**: Full audit & rollback capabilities
- 🚀 **Scalable**: Built on Delta Lake for enterprise reliability

**Final Checklist:**
- ✅ Bronze: Ingest JSON + metadata (_ingested_at, _source)
- ✅ Silver: 3+ cleaning transformations (dedup, null filter, type cast)
- ✅ Gold: 2 analytics tables + window functions + cross-join
- ✅ Time Travel: Update → query old version → compare
- ✅ README: Architecture diagram + justification
- ✅ Bonus: Ready for dashboard integration

---

*Created for ETS Big Data Course - Medallion Data Lakehouse*
