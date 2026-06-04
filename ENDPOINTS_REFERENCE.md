# 📡 API Endpoints Reference

## Dashboard API Endpoints - Complete List

Semua endpoints tersedia di `http://localhost:5000/api/`

---

## 🔴 KAFKA CONSUMER ENDPOINTS (Real-time Data)

### `GET /api/current_weather`
**Source**: Real-time Kafka `weather-api` topic  
**Update Frequency**: ~10 detik (auto-refresh di frontend)

**Response Format**:
```json
{
  "JKT": {
    "kode_kota": "JKT",
    "nama_kota": "Jakarta",
    "temperature": 28.5,
    "humidity": 75,
    "wind_speed": 10.5,
    "weather_code": 0,
    "weather_desc": "Cerah",
    "timestamp": "2026-06-04T14:30:45",
    "temp_color": "normal",
    "is_extreme": false,
    "latitude": -6.21,
    "longitude": 106.85
  },
  "SBY": { ... },
  ...
}
```

**Dashboard Section**: "🌡️ Status Cuaca Kota-Kota" (Weather Table)

---

### `GET /api/extreme_weather`
**Source**: Real-time data dengan kondisi ekstrem  
**Criteria**: `temperature > 35°C` OR `humidity > 90%` OR `wind_speed > 40 km/h`

**Response Format**:
```json
{
  "extreme_cities": [
    {
      "kode": "JKT",
      "nama": "Jakarta",
      "temperature": 36.5,
      "humidity": 92,
      "wind_speed": 45,
      "timestamp": "2026-06-04T14:30:45",
      "reason": [
        "Suhu 36.5°C > 35°C",
        "Kelembaban 92% > 90%",
        "Angin 45 km/h > 40 km/h"
      ],
      "severity": "tinggi"
    }
  ],
  "total_extreme": 1,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Dashboard Section**: "🚨 Peringatan Cuaca Ekstrem" (Extreme Weather Alert)

---

### `GET /api/latest_news`
**Source**: Real-time Kafka `weather-rss` topic  
**Returns**: Top 10 berita terbaru

**Response Format**:
```json
{
  "news": [
    {
      "judul": "Cuaca Ekstrem Terjadi di Jawa Barat",
      "link": "https://...",
      "ringkasan": "Suhu mencapai 37°C dengan angin kencang...",
      "sumber": "antaranews.com",
      "waktu_terbit": "2026-06-04T14:15:30"
    },
    ...
  ],
  "total": 10,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Dashboard Section**: "📰 Berita Cuaca Terbaru" (Latest News)

---

### `GET /api/spark_results`
**Source**: Cached Spark analysis results dari JSON file  
**Status**: `202 Pending` jika data belum tersedia

**Response Format**:
```json
{
  "status": "success",
  "data": {
    "suhu_kota": [
      {
        "nama_kota": "Jakarta",
        "suhu_avg": 28.8,
        "suhu_tertinggi": 35.2,
        "suhu_terendah": 22.5
      }
    ],
    "ekstrem": [...],
    "tren_jam": [...]
  },
  "timestamp": "2026-06-04T14:30:45"
}
```

**Dashboard Section**: "📊 Analisis Data Spark"

---

## 🥇 GOLD LAYER ENDPOINTS (Medallion Analytics)

### `GET /api/gold/weather_analytics` ⭐
**Source**: Delta Lake table `/lakehouse/gold/weather_analytics`  
**Type**: Time-series analysis per kota dengan window functions  
**Update Frequency**: Sesuai medallion pipeline schedule

**Response Format**:
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
    },
    {
      "kode_kota": "SBY",
      "nama_kota": "Surabaya",
      "temp_current": 31.2,
      "temp_change_1h": -0.5,
      "temp_moving_avg_3h": 31.5,
      "humidity": 82,
      "wind_speed": 8.2,
      "reading_time": "2026-06-04T14:30:00"
    },
    ...
  ],
  "count": 6,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Analysis Features**:
- ✅ `temp_current`: Suhu terkini per kota
- ✅ `temp_change_1h`: Perubahan suhu dalam 1 jam terakhir (deteksi rapid changes)
- ✅ `temp_moving_avg_3h`: Moving average suhu 3 jam (smooth trends)
- ✅ `humidity`, `wind_speed`: Kondisi cuaca real-time

**Use Cases**:
- Trend analysis per kota
- Detect rapid temperature swings
- Compare weather patterns across cities

**Dashboard Section**: "⏱️ Weather Time-Series Analytics (per Kota)"

---

### `GET /api/gold/weather_extremes` ⭐
**Source**: Delta Lake table `/lakehouse/gold/weather_extremes`  
**Type**: Summary statistics per kota  
**Computation**: GROUP BY kota dengan MAX, MIN, AVG aggregations

**Response Format**:
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
    },
    ...
  ],
  "count": 6,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Analysis Features**:
- ✅ `max_temp`, `min_temp`, `avg_temp`: Temperature statistics
- ✅ `temp_range`: max - min (volatility indicator)
- ✅ `max_humidity`, `max_wind`: Extreme conditions summary
- ✅ `reading_count`: Data quality indicator

**Use Cases**:
- City climate overview
- Extreme weather detection
- Inter-city temperature comparison

**Dashboard Section**: "🌡️ Weather Extremes Summary (per Kota)"

---

### `GET /api/gold/news_analytics` ⭐
**Source**: Delta Lake table `/lakehouse/gold/news_by_source`  
**Type**: News distribution analysis  
**Computation**: GROUP BY source dengan COUNT & MAX(timestamp)

**Response Format**:
```json
{
  "status": "success",
  "data": [
    {
      "sumber": "antaranews.com",
      "article_count": 42,
      "latest_article": "2026-06-04T14:15:30"
    },
    {
      "sumber": "mongabay.co.id",
      "article_count": 28,
      "latest_article": "2026-06-04T14:10:15"
    }
  ],
  "count": 2,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Analysis Features**:
- ✅ `sumber`: News source identifier
- ✅ `article_count`: Total articles from source
- ✅ `latest_article`: Most recent article timestamp

**Use Cases**:
- Source reliability ranking
- Content volume tracking
- Coverage completeness assessment

**Dashboard Section**: "📰 News Source Distribution (Gold Analytics)"

---

### `GET /api/gold/recent_news` ⭐
**Source**: Delta Lake table `/lakehouse/gold/recent_news`  
**Type**: Ranked recent articles (Top 20)  
**Filter**: `news_rank <= 20`

**Response Format**:
```json
{
  "status": "success",
  "data": [
    {
      "news_rank": 1,
      "judul": "Gelombang Panas Melanda Jawa Barat",
      "sumber": "antaranews.com",
      "waktu_terbit": "2026-06-04T14:15:30",
      "ringkasan": "Suhu mencapai 37°C dengan potensi banjir..."
    },
    {
      "news_rank": 2,
      "judul": "Konservasi Hutan Tropis Dipercepat",
      "sumber": "mongabay.co.id",
      "waktu_terbit": "2026-06-04T14:10:15",
      "ringkasan": "Pemerintah alokasikan Dana khusus..."
    },
    ...
  ],
  "count": 20,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Analysis Features**:
- ✅ Ranking indicator (relevance or recency)
- ✅ Full article details (title, summary, source)
- ✅ Publication timestamp for timeline analysis

**Use Cases**:
- Recent events feed
- Trending topics identification
- News aggregation source

**Dashboard Section**: "⏱️ Weather Time-Series Analytics" (supports additional tab)

---

### `GET /api/gold/weather_news_correlation` ⭐⭐
**Source**: Delta Lake table `/lakehouse/gold/weather_news_correlation`  
**Type**: Cross-domain correlation analysis  
**Computation**: Window functions (hourly) untuk weather aggregation + news count

**Response Format**:
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
    },
    {
      "kode_kota": "JKT",
      "nama_kota": "Jakarta",
      "hour_start": "2026-06-04T11:00:00",
      "hour_end": "2026-06-04T12:00:00",
      "avg_temp": 30.1,
      "max_humidity": 80,
      "max_wind": 12.5,
      "news_count": 1
    },
    {
      "kode_kota": "SBY",
      "nama_kota": "Surabaya",
      "hour_start": "2026-06-04T10:00:00",
      "hour_end": "2026-06-04T11:00:00",
      "avg_temp": 32.3,
      "max_humidity": 85,
      "max_wind": 18.7,
      "news_count": 5
    },
    ...
  ],
  "count": 144,
  "timestamp": "2026-06-04T14:30:45"
}
```

**Analysis Features** (Correlation Insights):
- ✅ `avg_temp` + `news_count`: Does heat trigger news?
- ✅ `max_humidity` + `news_count`: Does wet weather generate coverage?
- ✅ `max_wind` + `news_count`: Do storms produce headlines?
- ✅ Hourly granularity: Detect temporal patterns

**Visualizations**:
- Dual-axis chart: Temperature (left Y) vs News Count (right Y)
- X-axis: Time (hourly)
- Trend lines for both metrics

**Use Cases**:
- Discover weather-news correlations
- Predict news volume from weather events
- Anomaly detection (unusual weather without news coverage)
- Editorial bias detection (same event, different coverage)

**Dashboard Section**: "🔗 Weather-News Correlation" (Dual-axis chart + Table)

---

## 🔧 HEALTH & STATUS ENDPOINTS

### `GET /api/health`
**Purpose**: Server health check + component status

**Response Format**:
```json
{
  "status": "ok",
  "timestamp": "2026-06-04T14:30:45",
  "cities_with_data": 6,
  "news_count": 127,
  "gold_layer_available": true
}
```

**Key Indicators**:
- ✅ `status`: Server status (ok/error)
- ✅ `cities_with_data`: How many cities have real-time data
- ✅ `news_count`: Current news items in memory
- ✅ `gold_layer_available`: Spark session initialized?

**Use Cases**:
- Monitor dashboard availability
- Verify Spark/medallion integration
- Track real-time data ingestion

---

## 📊 Dashboard Section → Endpoint Mapping

| Dashboard Section | Endpoint(s) | Type | Update Frequency |
|-------------------|-------------|------|------------------|
| 🚨 Extreme Weather Alerts | `/api/extreme_weather` | Real-time | 10 sec |
| 🌡️ Status Cuaca Kota | `/api/current_weather` | Real-time | 10 sec |
| ⏱️ Time-Series Analytics | `/api/gold/weather_analytics` | Gold layer | Hourly |
| 🌡️ Weather Extremes | `/api/gold/weather_extremes` | Gold layer | Daily |
| 📰 News Source Dist. | `/api/gold/news_analytics` | Gold layer | Hourly |
| 🔗 Weather-News Correlation | `/api/gold/weather_news_correlation` | Gold layer | Hourly |
| 📰 Latest News | `/api/latest_news` | Real-time | 5 min |

---

## 🚀 Example cURL Requests

### Get Current Weather
```bash
curl -X GET http://localhost:5000/api/current_weather | jq
```

### Get Gold Layer Weather Analytics
```bash
curl -X GET http://localhost:5000/api/gold/weather_analytics | jq '.data[] | {kota: .nama_kota, temp: .temp_current, change: .temp_change_1h}'
```

### Get Weather Extremes
```bash
curl -X GET http://localhost:5000/api/gold/weather_extremes | jq '.data[] | {kota: .nama_kota, max: .max_temp, min: .min_temp, range: .temp_range}'
```

### Get Weather-News Correlation (specific hour)
```bash
curl -X GET http://localhost:5000/api/gold/weather_news_correlation | \
  jq '.data[] | select(.hour_start | contains("10:00")) | {jam: .hour_start, temp: .avg_temp, news: .news_count}'
```

### Check Health
```bash
curl -X GET http://localhost:5000/api/health | jq
```

---

## 📈 Response Status Codes

| Code | Meaning | Example |
|------|---------|---------|
| **200** | Success | Data available now |
| **202** | Accepted/Pending | Gold table not yet computed |
| **503** | Service Unavailable | Spark session not initialized |

---

## 🔄 Frontend Auto-Refresh Schedule

Configured in `dashboard/templates/index.html`:

```javascript
const CONFIG = {
  refreshInterval: 10000,  // 10 seconds
  apiBase: '/api'
};
```

**Refresh Pattern**:
```
Every 10 seconds:
├─ fetchCurrentWeather()
├─ fetchExtremeWeather()
├─ fetchSparkResults()
├─ fetchLatestNews()
├─ fetchGoldWeatherAnalytics()        // NEW
├─ fetchGoldWeatherExtremes()         // NEW
├─ fetchGoldNewsAnalytics()           // NEW
└─ fetchGoldCorrelation()             // NEW
```

---

## 📝 Notes

- All responses include `timestamp` field showing server time
- Times are in ISO 8601 format (UTC)
- Gold layer endpoints return `status` field ("success" or "pending")
- Real-time endpoints may return partial data if still loading
- Dashboard automatically handles 202 responses and retries

---

**Last Updated**: 2026-06-04  
**API Version**: 1.0  
**Maintained By**: WeatherPulse Team
