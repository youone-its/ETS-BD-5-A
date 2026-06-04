# 🥇 Medallion Architecture ↔ Dashboard Integration

## Gambaran Umum

Dashboard WeatherPulse kini terintegrasi penuh dengan **Medallion Architecture (Bronze-Silver-Gold)** menggunakan Delta Lake. Artinya, dashboard tidak hanya menampilkan data real-time dari Kafka, tetapi juga mengakses analytics yang sudah diolah di Gold layer.

## Arsitektur Data

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                 │
│         Open-Meteo API  │  RSS Feeds  │  Manual Data            │
└────────────┬─────────────────────────┬──────────────────────────┘
             │                         │
             ▼                         ▼
┌─────────────────────────────────────────────────────────────────┐
│      KAFKA TOPICS (Real-time Streaming)                         │
│    weather-api  │  weather-rss  │  Other streams                │
└────────────┬──────────────────────────────────────────────────┬─┘
             │                                                  │
    ┌────────▼────────────┐                         ┌──────────▼──────────┐
    │  KAFKA CONSUMER     │                         │  KAFKA CONSUMER     │
    │  (Dashboard +       │                         │  (HDFS Writer)      │
    │   Kafka Stream)     │                         │                     │
    └────────┬────────────┘                         └──────────┬──────────┘
             │                                                 │
             ▼                                                 ▼
    ┌─────────────────────┐                         ┌──────────────────────┐
    │   DASHBOARD APP     │                         │  HDFS RAW DATA       │
    │   (Real-time View)  │                         │  (Kafka Topics)      │
    └─────────────────────┘                         └──────────┬───────────┘
                                                               │
                                                    ┌──────────▼──────────┐
                                                    │   MEDALLION LAYERS  │
                                                    │                     │
                                        ┌───────────┴──────────────────┐  
                                        │                              │
                            ┌───────────▼────────┐    ┌───────────────▼─────────┐
                            │ BRONZE LAYER       │    │ SILVER LAYER            │
                            │ (/lakehouse/bronze)│    │ (/lakehouse/silver)     │
                            │                    │    │                         │
                            │ - weather_api      │───▶│ - weather_api (clean)   │
                            │ - weather_rss      │    │ - weather_rss (clean)   │
                            │ (Raw data)         │    │ (Dedup + Validation)    │
                            └────────────────────┘    └───────────┬─────────────┘
                                                                   │
                                                     ┌─────────────▼──────────┐
                                                     │ GOLD LAYER             │
                                                     │ (/lakehouse/gold)      │
                                                     │                        │
                                                     │ ✨ ANALYTICS TABLES:   │
                                                     │ - weather_analytics    │
                                                     │ - weather_extremes     │
                                                     │ - news_by_source       │
                                                     │ - recent_news          │
                                                     │ - weather_news_correlation
                                                     │                        │
                                                     │ (Business Insights)    │
                                                     └─────────────┬──────────┘
                                                                   │
                            ┌──────────────────────────────────────▼──────────┐
                            │          DASHBOARD API ENDPOINTS                │
                            │  (Fetch + Display Gold Layer Analytics)         │
                            └──────────────────────────────────────────────────┘
                                            │
                            ┌───────────────▼────────────────┐
                            │     FRONTEND (Browser)         │
                            │  Visualisasi & Real-time View  │
                            └────────────────────────────────┘
```

## Gold Layer Tables

### 1. **weather_analytics**
**Lokasi**: `/lakehouse/gold/weather_analytics`

Time-series analysis per kota dengan window functions:
- `temp_current`: Suhu terkini
- `temp_change_1h`: Perubahan suhu dalam 1 jam terakhir
- `temp_moving_avg_3h`: Moving average suhu 3 jam
- `humidity`, `wind_speed`: Kondisi cuaca terkini
- `reading_time`: Timestamp pengukuran

**API Endpoint**: `GET /api/gold/weather_analytics`

```json
{
  "status": "success",
  "data": [
    {
      "kode_kota": "JKT",
      "nama_kota": "Jakarta",
      "temp_current": 28.5,
      "temp_change_1h": 0.3,
      "temp_moving_avg_3h": 28.2,
      "humidity": 75,
      "wind_speed": 10.5,
      "reading_time": "2026-06-04T14:30:00"
    }
  ]
}
```

---

### 2. **weather_extremes**
**Lokasi**: `/lakehouse/gold/weather_extremes`

Summary statistik temperatur & kondisi ekstrem per kota:
- `max_temp`, `min_temp`, `avg_temp`: Statistik suhu
- `temp_range`: Selisih suhu max-min
- `max_humidity`, `max_wind`: Kondisi ekstrem
- `reading_count`: Jumlah pembacaan yang dianalisis

**API Endpoint**: `GET /api/gold/weather_extremes`

```json
{
  "status": "success",
  "data": [
    {
      "kode_kota": "JKT",
      "nama_kota": "Jakarta",
      "max_temp": 35.2,
      "min_temp": 22.5,
      "avg_temp": 28.8,
      "temp_range": 12.7,
      "max_humidity": 92,
      "max_wind": 45.3,
      "reading_count": 144
    }
  ]
}
```

---

### 3. **news_by_source**
**Lokasi**: `/lakehouse/gold/news_by_source`

Distribusi artikel per sumber RSS:
- `sumber`: Nama sumber berita
- `article_count`: Jumlah artikel dari sumber tersebut
- `latest_article`: Waktu artikel terbaru

**API Endpoint**: `GET /api/gold/news_analytics`

```json
{
  "status": "success",
  "data": [
    {
      "sumber": "antaranews.com",
      "article_count": 42,
      "latest_article": "2026-06-04T14:15:00"
    },
    {
      "sumber": "mongabay.co.id",
      "article_count": 28,
      "latest_article": "2026-06-04T14:10:00"
    }
  ]
}
```

---

### 4. **recent_news**
**Lokasi**: `/lakehouse/gold/recent_news`

Top 20 berita terbaru dengan ranking:
- `news_rank`: Ranking (1-20)
- `judul`, `ringkasan`: Konten artikel
- `sumber`: Asal berita
- `waktu_terbit`: Timestamp publikasi

**API Endpoint**: `GET /api/gold/recent_news`

---

### 5. **weather_news_correlation**
**Lokasi**: `/lakehouse/gold/weather_news_correlation`

Analisis korelasi antara kondisi cuaca & volume berita (per jam):
- `hour`: Window hourly (start & end timestamp)
- `kode_kota`, `nama_kota`: Lokasi
- `avg_temp`, `max_humidity`, `max_wind`: Kondisi cuaca
- `news_count`: Jumlah berita terbit dalam jam tersebut

**Insight**: Apakah cuaca ekstrem berkorelasi dengan lonjakan berita?

**API Endpoint**: `GET /api/gold/weather_news_correlation`

```json
{
  "status": "success",
  "data": [
    {
      "kode_kota": "JKT",
      "nama_kota": "Jakarta",
      "hour_start": "2026-06-04T10:00:00",
      "hour_end": "2026-06-04T11:00:00",
      "avg_temp": 28.5,
      "max_humidity": 88,
      "max_wind": 15.2,
      "news_count": 3
    }
  ]
}
```

---

## Dashboard Integration

### Kafka Consumer Endpoints (Real-time)
```
GET /api/current_weather      → Latest weather per kota (real-time)
GET /api/extreme_weather      → Current extreme weather alerts
GET /api/latest_news          → Top 10 berita dari Kafka stream
```

### Gold Layer Endpoints (Analytics)
```
GET /api/gold/weather_analytics      → Time-series weather analysis
GET /api/gold/weather_extremes       → Weather extremes summary
GET /api/gold/news_analytics         → News source distribution
GET /api/gold/recent_news            → Top 20 recent articles
GET /api/gold/weather_news_correlation → Weather-news correlation
```

### Health & Status
```
GET /api/health               → Server status + Spark session availability
```

## Frontend Sections

Dashboard HTML sudah di-update dengan 4 section baru untuk Gold layer:

1. **⏱️ Weather Time-Series Analytics**
   - Menampilkan tren suhu per kota (perubahan 1 jam, moving average 3 jam)
   - Source: `/api/gold/weather_analytics`

2. **🌡️ Weather Extremes Summary**
   - Statistik min/max/avg temperature, temp range, humidity & wind
   - Source: `/api/gold/weather_extremes`

3. **📰 News Source Distribution**
   - Bar chart/table menampilkan artikel count per sumber
   - Source: `/api/gold/news_analytics`

4. **🔗 Weather-News Correlation**
   - Dual-axis chart menampilkan weather trends vs news volume
   - Table detail per jam per kota
   - Source: `/api/gold/weather_news_correlation`

---

## Running the Integration

### Prerequisites
1. **Medallion pipeline sudah berjalan**:
   - Bronze, Silver, Gold layers terisi
   - Delta tables tersedia di `/lakehouse/gold/`

2. **Spark dan Delta Lake tersedia**:
   ```bash
   pip install pyspark delta-spark
   ```

3. **Kafka running** (untuk real-time data)

### Startup

```bash
# 1. Jalankan medallion pipeline (jika belum)
cd /path/to/project/medallion
python 01_bronze.py
python 02_silver.py
python 03_gold.py

# 2. Start Kafka consumers (producer pipeline)
python kafka/producer_api.py &
python kafka/producer_rss.py &
python kafka/consumer_to_hdfs.py &

# 3. Start dashboard server
cd dashboard
python app.py
```

**Output**:
```
======================================================================
⛅ WeatherPulse Dashboard Server - with Medallion Integration
======================================================================
✨ Server starting at http://localhost:5000
📊 Kafka Consumer Endpoints (Real-time):
   - GET /api/current_weather
   - GET /api/extreme_weather
   - GET /api/latest_news
🥇 Gold Layer Analytics Endpoints:
   - GET /api/gold/weather_analytics (time-series analysis)
   - GET /api/gold/weather_extremes (temperature ranges)
   - GET /api/gold/news_analytics (source distribution)
   - GET /api/gold/recent_news (top 20 articles)
   - GET /api/gold/weather_news_correlation (weather-news correlation)
✅ Spark session ready - Gold layer analytics enabled
======================================================================
```

### Verify Integration

**1. Check Spark session**:
```bash
curl http://localhost:5000/api/health
# Response: { "status": "ok", "gold_layer_available": true, ... }
```

**2. Test Gold layer endpoints**:
```bash
# Weather analytics
curl http://localhost:5000/api/gold/weather_analytics

# Weather extremes
curl http://localhost:5000/api/gold/weather_extremes

# News analytics
curl http://localhost:5000/api/gold/news_analytics

# Correlation
curl http://localhost:5000/api/gold/weather_news_correlation
```

**3. Open browser**:
```
http://localhost:5000
```

Dashboard akan menampilkan:
- Real-time weather table (dari Kafka)
- Extreme weather alerts (dari Kafka)
- Latest news (dari Kafka)
- **Gold layer analytics** (4 section baru dengan chart & tables)

---

## Architecture Benefits

✅ **Real-time + Analytics**: Kombinasi Kafka stream + Gold layer
✅ **Scalability**: Delta Lake memungkinkan data besar (billions of rows)
✅ **ACID Transactions**: Delta memberi jaminan consistency
✅ **Time-travel**: Bisa query data di waktu lampau dengan `VERSION AS OF`
✅ **Window Functions**: Advanced analytics dengan Spark SQL
✅ **Correlation Analysis**: Discover insights antara weather & news
✅ **Business Intelligence**: Pre-computed analytics siap pakai di dashboard

---

## Troubleshooting

### ❌ Gold layer endpoints return 503 (Spark unavailable)
- Check: Spark session initialization
- Solution: Install pyspark, delta-spark, atau set LAKEHOUSE_PATH env var
  ```bash
  export LAKEHOUSE_PATH=/lakehouse
  python app.py
  ```

### ❌ Gold layer endpoints return 202 (pending)
- Check: Gold tables sudah ada di `/lakehouse/gold/`
- Solution: Run medallion gold layer script terlebih dahulu
  ```bash
  python medallion/03_gold.py
  ```

### ❌ Correlation chart not showing
- Check: `weather_news_correlation` table exists & has data
- Check: Browser console for JavaScript errors
- Solution: Ensure `weather_rss` & `weather_api` data sudah ada

---

## Next Steps

1. **Real-time updates**: Implementasi WebSocket/Server-Sent Events untuk live updates
2. **Advanced analytics**: Tambah machine learning models (time series forecasting)
3. **Data retention**: Setup Delta lifecycle management untuk old data
4. **Performance**: Implement caching di Redis untuk frequently accessed queries
5. **Alerting**: Trigger alerts berdasarkan Gold layer anomalies

---

**Last Updated**: 2026-06-04
**Author**: WeatherPulse Team
