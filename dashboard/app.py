"""
WeatherPulse Dashboard - Flask Backend
Balqis Sani Sabillah - 5027241002

Features:
- Real-time weather data dari Kafka (weather-api topic)
- Latest news dari Kafka (weather-rss topic)
- Spark analysis results dari JSON
- Gold Layer Analytics dari Delta Lake (via pandas + pyarrow)
- Auto-refresh endpoints untuk frontend
"""

import glob
import json
import os
import sys
from datetime import datetime, timedelta
from threading import Thread
from collections import deque, defaultdict
from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TIMEOUT_MS = 5000

# Path untuk Spark results (lama)
SPARK_RESULTS_PATH = os.path.join(os.path.dirname(__file__), "data", "spark_results.json")

# Path untuk Gold Delta Lake tables (baru)
GOLD_PATH = os.environ.get("GOLD_PATH", "/lakehouse_data/gold")

# Koordinat kota untuk referensi
CITIES_INFO = {
    "JKT": {"nama": "Jakarta", "lat": -6.21, "lon": 106.85},
    "SBY": {"nama": "Surabaya", "lat": -7.25, "lon": 112.75},
    "SMG": {"nama": "Semarang", "lat": -6.99, "lon": 110.42},
    "MDN": {"nama": "Medan", "lat": -3.59, "lon": 98.67},
    "MKS": {"nama": "Makassar", "lat": -5.14, "lon": 119.41},
    "DPS": {"nama": "Denpasar", "lat": -8.67, "lon": 115.21}
}

# ============================================================================
# FLASK APP INITIALIZATION
# ============================================================================

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# ============================================================================
# GLOBAL STATE MANAGEMENT
# ============================================================================

# Store latest weather data per kota (keep last 100 readings)
latest_weather = {}
weather_history = defaultdict(lambda: deque(maxlen=100))

# Store latest news (keep last 20)
latest_news = deque(maxlen=20)

# ============================================================================
# GOLD DELTA LAKE READER
# ============================================================================

def read_gold_table(table_name):
    """
    Baca Gold Delta table sebagai pandas DataFrame.
    Delta tables tersimpan sebagai parquet files + _delta_log.
    Kita baca semua *.parquet di direktori tersebut (skip _delta_log).
    """
    try:
        import pandas as pd
        path = os.path.join(GOLD_PATH, table_name)
        if not os.path.exists(path):
            print(f"[Dashboard] ⚠️  Gold table tidak ditemukan: {path}")
            return None

        parquet_files = glob.glob(os.path.join(path, "*.parquet"))
        if not parquet_files:
            print(f"[Dashboard] ⚠️  Tidak ada parquet files di: {path}")
            return None

        dfs = [pd.read_parquet(f) for f in parquet_files]
        df = pd.concat(dfs, ignore_index=True) if dfs else None
        print(f"[Dashboard] ✅ Gold table '{table_name}' loaded: {len(df)} rows")
        return df

    except ImportError:
        print("[Dashboard] ❌ pandas/pyarrow tidak terinstall. Jalankan: pip install pandas pyarrow")
        return None
    except Exception as e:
        print(f"[Dashboard] ❌ Error membaca gold table '{table_name}': {e}")
        return None


def df_to_records(df):
    """
    Konversi DataFrame ke list of dicts yang JSON-serializable.
    Handle: Timestamp, NaN, numpy types, struct (dict) columns.
    """
    if df is None:
        return []
    try:
        import numpy as np
        import pandas as pd

        result = []
        for _, row in df.iterrows():
            record = {}
            for col_name, val in row.items():
                # Skip NaN
                if isinstance(val, float) and (val != val):  # NaN check
                    record[col_name] = None
                elif isinstance(val, pd.Timestamp):
                    record[col_name] = val.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(val, datetime):
                    record[col_name] = val.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(val, np.integer):
                    record[col_name] = int(val)
                elif isinstance(val, np.floating):
                    record[col_name] = None if np.isnan(val) else float(val)
                elif isinstance(val, np.bool_):
                    record[col_name] = bool(val)
                elif isinstance(val, dict):
                    # Handle struct types (e.g., window() returns {start, end})
                    cleaned = {}
                    for k, v in val.items():
                        if isinstance(v, pd.Timestamp):
                            cleaned[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            cleaned[k] = v
                    record[col_name] = cleaned
                else:
                    record[col_name] = val
            result.append(record)
        return result
    except Exception as e:
        print(f"[Dashboard] ❌ Error converting df to records: {e}")
        return []

# ============================================================================
# KAFKA CONSUMER THREADS
# ============================================================================

def consume_weather_api():
    """
    Background thread: Consumer untuk topic 'weather-api'
    Update latest_weather dengan data terbaru per kota
    """
    try:
        consumer = KafkaConsumer(
            'weather-api',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=KAFKA_TIMEOUT_MS,
            group_id='dashboard-api-consumer-v2'
        )
        
        print("[Dashboard] 🌡️ Weather API Consumer started...")
        
        for message in consumer:
            data = message.value
            kode_kota = data.get('kode_kota')
            
            if kode_kota:
                # Update latest reading
                latest_weather[kode_kota] = data
                
                # Keep history
                weather_history[kode_kota].append(data)
                
                print(f"[Dashboard] Updated {kode_kota}: {data['temperature']}°C, "
                      f"humidity: {data['humidity']}%, wind: {data['wind_speed']} km/h")
    
    except KafkaError as e:
        print(f"[Dashboard] ❌ Kafka error in weather consumer: {e}")
    except Exception as e:
        print(f"[Dashboard] ❌ Error in weather consumer: {e}")
    finally:
        consumer.close()

def consume_weather_rss():
    """
    Background thread: Consumer untuk topic 'weather-rss'
    Update latest_news dengan artikel terbaru
    """
    try:
        consumer = KafkaConsumer(
            'weather-rss',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=KAFKA_TIMEOUT_MS,
            group_id='dashboard-rss-consumer-baru-banget'
        )
        
        print("[Dashboard] 📰 Weather RSS Consumer started...")
        
        for message in consumer:
            artikel = message.value
            latest_news.appendleft(artikel)  # Newest first
            
            print(f"[Dashboard] New article: {artikel['judul'][:50]}...")
    
    except KafkaError as e:
        print(f"[Dashboard] ❌ Kafka error in RSS consumer: {e}")
    except Exception as e:
        print(f"[Dashboard] ❌ Error in RSS consumer: {e}")
    finally:
        consumer.close()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_temperature_color(temp):
    """
    Determine CSS color class based on temperature
    Dingin (< 20): blue
    Normal (20-30): green
    Panas (30-35): orange
    Ekstrem (> 35): red
    """
    if temp < 20:
        return "cold"
    elif temp < 25:
        return "cool"
    elif temp < 30:
        return "normal"
    elif temp < 35:
        return "warm"
    else:
        return "hot"

def get_weather_description(weather_code):
    """
    WMO Weather interpretation codes
    Ref: https://www.open-meteo.com/en/docs
    """
    codes = {
        0: "Cerah",
        1: "Sebagian berawan",
        2: "Berawan",
        3: "Sangat berawan",
        45: "Berkabut",
        48: "Kabut salju",
        51: "Gerimis ringan",
        53: "Gerimis sedang",
        55: "Gerimis lebat",
        61: "Hujan ringan",
        63: "Hujan sedang",
        65: "Hujan lebat",
        71: "Salju ringan",
        73: "Salju sedang",
        75: "Salju lebat",
        77: "Butir salju",
        80: "Hujan ringan yang terputus",
        81: "Hujan sedang yang terputus",
        82: "Hujan lebat yang terputus",
        85: "Salju ringan yang terputus",
        86: "Salju lebat yang terputus",
        95: "Badai petir",
        96: "Badai petir ringan",
        99: "Badai petir berat"
    }
    return codes.get(weather_code, "Tidak diketahui")

def load_spark_results():
    """
    Load Spark analysis results dari JSON file (legacy)
    Return dict dengan struktur: {
        "suhu_kota": [...],
        "ekstrem": [...],
        "tren_jam": [...]
    }
    """
    if os.path.exists(SPARK_RESULTS_PATH):
        try:
            with open(SPARK_RESULTS_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"[Dashboard] ⚠️ Error loading spark results: {e}")
            return None
    return None

def is_extreme_weather(temp, humidity, wind_speed):
    """
    Check if weather is extreme condition
    Criteria: wind_speed > 40 OR humidity > 90 OR temperature > 35
    """
    return wind_speed > 40 or humidity > 90 or temp > 35

def gold_table_available():
    """Cek apakah minimal 1 Gold table tersedia"""
    for table in ["weather_extremes", "recent_news", "news_by_source",
                  "weather_news_correlation", "weather_analytics"]:
        path = os.path.join(GOLD_PATH, table)
        if os.path.exists(path):
            return True
    return False

# ============================================================================
# FLASK ROUTES — EXISTING (TIDAK DIUBAH)
# ============================================================================

@app.route('/')
def index():
    """Render dashboard homepage"""
    return render_template('index.html')

@app.route('/api/current_weather')
def api_current_weather():
    """
    Return latest weather data untuk semua kota.
    Prioritas:
    1. Kafka latest_weather
    2. Gold weather_analytics
    3. Placeholder
    """

    result = {}

    # ==========================================================
    # Fallback ke Gold Layer jika Kafka tidak ada data
    # ==========================================================
    # ==========================================================
# PRIORITY DATA SOURCE: Kafka → Gold → Placeholder
# ==========================================================

df = None

# 1. Coba Kafka dulu
if latest_weather:
    source = "kafka"
else:
    # 2. Kalau Kafka kosong → Gold fallback
    df = read_gold_table("weather_analytics")

    if df is not None and len(df) > 0:
        import pandas as pd

        try:
            if "reading_time" in df.columns:
                df["reading_time"] = pd.to_datetime(df["reading_time"], errors="coerce")

                df = (
                    df.sort_values("reading_time", ascending=False)
                    .groupby("kode_kota")
                    .first()
                    .reset_index()
                )

            latest_weather.clear()   # 🔥 penting: reset biar clean

            for _, row in df.iterrows():
                latest_weather[row["kode_kota"]] = {
                    "kode_kota": row["kode_kota"],
                    "nama_kota": row.get("nama_kota", row["kode_kota"]),
                    "temperature": float(row.get("temp_current", 0)),
                    "humidity": float(row.get("humidity", 0)),
                    "wind_speed": float(row.get("wind_speed", 0)),
                    "weather_code": 0,
                    "timestamp": str(row.get("reading_time", "N/A"))
                }

            source = "gold"

        except Exception as e:
            print(f"[Dashboard] Gold fallback error: {e}")
            source = "empty"
    else:
        source = "empty"

        if df is not None and len(df) > 0:

            try:
                import pandas as pd

                if "reading_time" in df.columns:

                    df["reading_time"] = pd.to_datetime(
                        df["reading_time"],
                        errors="coerce"
                    )

                    df = (
                        df.sort_values(
                            "reading_time",
                            ascending=False
                        )
                        .groupby("kode_kota")
                        .first()
                        .reset_index()
                    )

                for _, row in df.iterrows():

                    latest_weather[row["kode_kota"]] = {
                        "kode_kota": row["kode_kota"],
                        "nama_kota": row.get(
                            "nama_kota",
                            row["kode_kota"]
                        ),
                        "temperature": float(
                            row.get("temp_current", 0)
                        ),
                        "humidity": float(
                            row.get("humidity", 0)
                        ),
                        "wind_speed": float(
                            row.get("wind_speed", 0)
                        ),
                        "weather_code": 0,
                        "timestamp": str(
                            row.get(
                                "reading_time",
                                "N/A"
                            )
                        )
                    }

                print(
                    f"[Dashboard] Loaded {len(latest_weather)} cities from Gold Layer"
                )

            except Exception as e:
                print(
                    f"[Dashboard] Gold fallback error: {e}"
                )

    # ==========================================================
    # Build response
    # ==========================================================
    for kode, info in CITIES_INFO.items():

        if kode in latest_weather:

            data = latest_weather[kode]

            temp = data.get("temperature", 0)
            humidity = data.get("humidity", 0)
            wind = data.get("wind_speed", 0)

            result[kode] = {
                "kode_kota": kode,
                "nama_kota": data.get(
                    "nama_kota",
                    info["nama"]
                ),
                "temperature": temp,
                "humidity": humidity,
                "wind_speed": wind,
                "weather_code": data.get(
                    "weather_code",
                    -1
                ),
                "weather_desc": get_weather_description(
                    data.get("weather_code", -1)
                ),
                "timestamp": data.get(
                    "timestamp",
                    "N/A"
                ),
                "temp_color": get_temperature_color(
                    temp
                ),
                "is_extreme": is_extreme_weather(
                    temp,
                    humidity,
                    wind
                ),
                "latitude": info["lat"],
                "longitude": info["lon"]
            }

        else:

            result[kode] = {
                "kode_kota": kode,
                "nama_kota": info["nama"],
                "temperature": None,
                "humidity": None,
                "wind_speed": None,
                "weather_code": -1,
                "weather_desc": "Menunggu data...",
                "timestamp": "N/A",
                "temp_color": "pending",
                "is_extreme": False,
                "latitude": info["lat"],
                "longitude": info["lon"]
            }

    print("=== CURRENT WEATHER DEBUG ===")
    print("latest_weather size:", len(latest_weather))
    print("source mode:", "kafka/gold/empty")

    return jsonify(result)

@app.route('/api/extreme_weather')
def api_extreme_weather():
    """
    Return list kota dengan kondisi ekstrem
    """
    extreme_cities = []
    
    for kode, data in latest_weather.items():
        temp = data.get('temperature', 0)
        humidity = data.get('humidity', 0)
        wind = data.get('wind_speed', 0)
        
        if is_extreme_weather(temp, humidity, wind):
            reasons = []
            if temp > 35:
                reasons.append(f"Suhu {temp}°C > 35°C")
            if humidity > 90:
                reasons.append(f"Kelembaban {humidity}% > 90%")
            if wind > 40:
                reasons.append(f"Angin {wind} km/h > 40 km/h")
            
            extreme_cities.append({
                'kode': kode,
                'nama': data.get('nama_kota', CITIES_INFO.get(kode, {}).get('nama')),
                'temperature': temp,
                'humidity': humidity,
                'wind_speed': wind,
                'timestamp': data.get('timestamp', 'N/A'),
                'reason': reasons,
                'severity': 'tinggi' if len(reasons) > 1 else 'sedang'
            })
    
    # Sort by severity (multiple reasons = higher priority)
    extreme_cities.sort(key=lambda x: -len(x['reason']))
    
    return jsonify({
        'extreme_cities': extreme_cities,
        'total_extreme': len(extreme_cities),
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/spark_results')
def api_spark_results():
    """
    Return Spark analysis results.
    UPDATED: Coba baca dari Gold Delta layer dulu, fallback ke spark_results.json
    3 analisis wajib:
    1. Perbandingan suhu antar kota
    2. Deteksi kondisi ekstrem
    3. Tren suhu per jam
    """
    # ── Coba Gold Layer dulu ──────────────────────────────────────────────
    df_extremes = read_gold_table("weather_extremes")
    df_analytics = read_gold_table("weather_analytics")

    if df_extremes is not None:
        import pandas as pd

        # Analisis 1: Perbandingan suhu antar kota
        suhu_kota = []
        for _, row in df_extremes.iterrows():
            suhu_kota.append({
                "nama_kota": str(row.get("nama_kota", row.get("kode_kota", "N/A"))),
                "suhu_avg": round(float(row["avg_temp"]), 2) if pd.notna(row.get("avg_temp")) else None,
                "suhu_tertinggi": round(float(row["max_temp"]), 2) if pd.notna(row.get("max_temp")) else None,
                "suhu_terendah": round(float(row["min_temp"]), 2) if pd.notna(row.get("min_temp")) else None,
            })

        # Analisis 2: Deteksi kondisi ekstrem per kota
        ekstrem = []
        for _, row in df_extremes.iterrows():
            events = 0
            if pd.notna(row.get("max_temp")) and float(row["max_temp"]) > 35:
                events += int(row.get("reading_count", 1))
            if pd.notna(row.get("max_humidity")) and float(row["max_humidity"]) > 90:
                events += 1
            if pd.notna(row.get("max_wind")) and float(row["max_wind"]) > 40:
                events += 1
            ekstrem.append({
                "nama_kota": str(row.get("nama_kota", row.get("kode_kota", "N/A"))),
                "jumlah_event_ekstrem": events,
                "max_temp": round(float(row["max_temp"]), 2) if pd.notna(row.get("max_temp")) else None,
                "max_humidity": int(row["max_humidity"]) if pd.notna(row.get("max_humidity")) else None,
                "max_wind": round(float(row["max_wind"]), 2) if pd.notna(row.get("max_wind")) else None,
            })

        # Analisis 3: Tren suhu per jam (dari weather_analytics jika ada)
        tren_jam = []
        if df_analytics is not None and "reading_time" in df_analytics.columns:
            try:
                df_analytics["reading_time"] = pd.to_datetime(df_analytics["reading_time"], errors="coerce")
                df_analytics["jam"] = df_analytics["reading_time"].dt.hour
                tren = df_analytics.groupby("jam")["temp_current"].mean().reset_index()
                tren = tren.sort_values("jam")
                for _, row in tren.iterrows():
                    tren_jam.append({
                        "jam": int(row["jam"]),
                        "suhu_avg": round(float(row["temp_current"]), 2)
                    })
            except Exception as e:
                print(f"[Dashboard] ⚠️ Tren jam error: {e}")

        return jsonify({
            'status': 'success',
            'source': 'gold_delta_lake',  # ← indicator bahwa data dari Gold layer
            'data': {
                'suhu_kota': suhu_kota,
                'ekstrem': ekstrem,
                'tren_jam': tren_jam
            },
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    # ── Fallback: spark_results.json (ETS lama) ──────────────────────────
    results = load_spark_results()
    if results:
        return jsonify({
            'status': 'success',
            'source': 'spark_results_json',
            'data': results,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Gold layer & Spark analysis belum tersedia. Jalankan pipeline medallion terlebih dahulu.',
            'source': None,
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

@app.route('/api/latest_news')
def api_latest_news():
    """
    Return latest news dari RSS feeds (Kafka).
    Jika Kafka kosong, coba dari Gold recent_news table.
    """
    news_list = list(latest_news)[:10]

    # Fallback ke Gold recent_news jika Kafka belum ada data
    if not news_list:
        df = read_gold_table("recent_news")
        if df is not None:
            import pandas as pd
            df = df.sort_values("news_rank") if "news_rank" in df.columns else df
            for _, row in df.head(10).iterrows():
                news_list.append({
                    "judul": str(row.get("judul", "")),
                    "link": str(row.get("link", "#")),
                    "ringkasan": str(row.get("ringkasan", "")),
                    "sumber": str(row.get("sumber", "N/A")),
                    "waktu_terbit": str(row.get("waktu_terbit", ""))
                        if not isinstance(row.get("waktu_terbit"), float) else "",
                })

    return jsonify({
        'news': news_list,
        'total': len(news_list),
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/health')
def api_health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'cities_with_data': len(latest_weather),
        'news_count': len(latest_news),
        'gold_layer_available': gold_table_available(),
        'gold_path': GOLD_PATH
    })

# ============================================================================
# FLASK ROUTES — GOLD LAYER (BARU)
# ============================================================================

@app.route('/api/gold/status')
def api_gold_status():
    """Cek tabel Gold mana yang tersedia"""
    tables = ["weather_analytics", "weather_extremes", "news_by_source",
              "recent_news", "weather_news_correlation"]
    status = {}
    for t in tables:
        path = os.path.join(GOLD_PATH, t)
        parquet_files = glob.glob(os.path.join(path, "*.parquet")) if os.path.exists(path) else []
        status[t] = {
            "available": len(parquet_files) > 0,
            "parquet_files": len(parquet_files),
            "path": path
        }
    return jsonify({
        "gold_path": GOLD_PATH,
        "tables": status,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/gold/weather_extremes')
def api_gold_weather_extremes():
    """
    Gold Table: Statistik cuaca ekstrem per kota
    Kolom: kode_kota, nama_kota, max_temp, min_temp, avg_temp,
           max_humidity, max_wind, reading_count, temp_range
    """
    df = read_gold_table("weather_extremes")
    if df is None:
        return jsonify({"status": "unavailable",
                        "message": "Tabel weather_extremes belum dibuat. Jalankan 03_gold.py",
                        "data": []})
    records = df_to_records(df)
    return jsonify({
        "status": "success",
        "source": "gold_delta_lake",
        "total": len(records),
        "data": records,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/gold/weather_analytics')
def api_gold_weather_analytics():
    """
    Gold Table: Time-series analytics per kota
    Kolom: kode_kota, nama_kota, temp_current, temp_change_1h,
           temp_moving_avg_3h, humidity, wind_speed, reading_time
    """
    df = read_gold_table("weather_analytics")
    if df is None:
        return jsonify({"status": "unavailable",
                        "message": "Tabel weather_analytics belum dibuat. Jalankan 03_gold.py",
                        "data": []})
    # Ambil latest reading per kota untuk ringkasan
    import pandas as pd
    if "kode_kota" in df.columns and "reading_time" in df.columns:
        try:
            df["reading_time"] = pd.to_datetime(df["reading_time"], errors="coerce")
            df = df.sort_values("reading_time", ascending=False)
            latest = df.groupby("kode_kota").first().reset_index()
            records = df_to_records(latest)
        except Exception:
            records = df_to_records(df.head(50))
    else:
        records = df_to_records(df.head(50))

    return jsonify({
        "status": "success",
        "source": "gold_delta_lake",
        "total": len(records),
        "data": records,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/gold/news_by_source')
def api_gold_news_by_source():
    """
    Gold Table: Distribusi berita per sumber RSS
    Kolom: sumber, article_count, latest_article
    """
    df = read_gold_table("news_by_source")
    if df is None:
        return jsonify({"status": "unavailable",
                        "message": "Tabel news_by_source belum dibuat. Jalankan 03_gold.py",
                        "data": []})
    # Sort by article count descending
    import pandas as pd
    if "article_count" in df.columns:
        df = df.sort_values("article_count", ascending=False)
    records = df_to_records(df)
    return jsonify({
        "status": "success",
        "source": "gold_delta_lake",
        "total": len(records),
        "data": records,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/gold/recent_news')
def api_gold_recent_news():
    """
    Gold Table: Top 20 artikel terbaru (ranked by waktu_terbit)
    Kolom: news_rank, judul, sumber, waktu_terbit, ringkasan
    """
    df = read_gold_table("recent_news")
    if df is None:
        return jsonify({"status": "unavailable",
                        "message": "Tabel recent_news belum dibuat. Jalankan 03_gold.py",
                        "data": []})
    import pandas as pd
    if "news_rank" in df.columns:
        df = df.sort_values("news_rank")
    records = df_to_records(df)
    return jsonify({
        "status": "success",
        "source": "gold_delta_lake",
        "total": len(records),
        "data": records,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/api/gold/weather_news_correlation')
def api_gold_weather_news_correlation():
    """
    Gold Table (Enhanced): Korelasi cuaca + berita per window 1 jam
    Kolom: kode_kota, nama_kota, hour (struct), avg_temp,
           max_humidity, max_wind, news_count
    """
    df = read_gold_table("weather_news_correlation")
    if df is None:
        return jsonify({"status": "unavailable",
                        "message": "Tabel weather_news_correlation belum dibuat. Jalankan 03_gold.py",
                        "data": []})
    import pandas as pd
    # Ambil top 50 rows berdasarkan news_count descending
    if "news_count" in df.columns:
        df = df.sort_values("news_count", ascending=False)
    records = df_to_records(df.head(50))
    return jsonify({
        "status": "success",
        "source": "gold_delta_lake",
        "total": len(records),
        "data": records,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# ============================================================================
# STARTUP & BACKGROUND THREADS
# ============================================================================

def start_background_consumers():
    """Start Kafka consumers in background threads"""
    
    # Start weather API consumer
    api_thread = Thread(target=consume_weather_api, daemon=True)
    api_thread.start()
    
    # Start RSS consumer
    rss_thread = Thread(target=consume_weather_rss, daemon=True)
    rss_thread.start()
    
    print("[Dashboard] 🚀 Background consumers started")

@app.before_request
def init_app():
    """Initialize app on first request"""
    if not hasattr(app, 'consumers_started'):
        start_background_consumers()
        app.consumers_started = True

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*70)
    print("⛅ WeatherPulse Dashboard Server")
    print("="*70)
    print(f"✨ Server starting at http://localhost:5000")
    print(f"📊 API endpoints:")
    print(f"   [LIVE]  GET /api/current_weather")
    print(f"   [LIVE]  GET /api/extreme_weather")
    print(f"   [LIVE]  GET /api/latest_news")
    print(f"   [GOLD]  GET /api/spark_results   (Gold layer dulu, fallback JSON)")
    print(f"   [GOLD]  GET /api/gold/status")
    print(f"   [GOLD]  GET /api/gold/weather_extremes")
    print(f"   [GOLD]  GET /api/gold/weather_analytics")
    print(f"   [GOLD]  GET /api/gold/news_by_source")
    print(f"   [GOLD]  GET /api/gold/recent_news")
    print(f"   [GOLD]  GET /api/gold/weather_news_correlation")
    print(f"   [SYS]   GET /api/health")
    print(f"📁 Gold Path: {GOLD_PATH}")
    print(f"🥇 Gold tables available: {gold_table_available()}")
    print("="*70 + "\n")
    
    # Run Flask server
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,  # Set True hanya untuk development
        use_reloader=False  # Important untuk background threads
    )
