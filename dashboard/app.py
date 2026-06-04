"""
WeatherPulse Dashboard - Flask Backend with Medallion Integration
Balqis Sani Sabillah - 5027241002

Features:
- Real-time weather data dari Kafka (weather-api topic)
- Latest news dari Kafka (weather-rss topic)
- Gold layer analytics dari Delta Lake medallion architecture
- Time-series analysis, weather-news correlation
- Auto-refresh endpoints untuk frontend
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from threading import Thread
from collections import deque, defaultdict
from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Delta Lake & Spark untuk medallion integration
try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️  Warning: PySpark tidak tersedia - Gold layer analytics akan disabled")

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_TIMEOUT_MS = 5000

# Path untuk Spark results
SPARK_RESULTS_PATH = os.path.join(os.path.dirname(__file__), "data", "spark_results.json")

# Medallion lakehouse path
LAKEHOUSE_PATH = os.getenv("LAKEHOUSE_PATH", "/lakehouse")
GOLD_PATH = f"{LAKEHOUSE_PATH}/gold"

# Spark session untuk Gold layer
spark_session = None
if SPARK_AVAILABLE:
    try:
        spark_session = SparkSession.builder \
            .appName("weatherpulse-dashboard") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        spark_session.sparkContext.setLogLevel("ERROR")
        print("✅ Spark session initialized for Gold layer access")
    except Exception as e:
        print(f"⚠️  Could not initialize Spark session: {e}")
        spark_session = None

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
# KAFKA CONSUMER THREADS
# ============================================================================

def consume_weather_api():
    """
    Background thread: Consumer untuk topic 'weather-api'
    Update latest_weather dengan data terbaru per kota
    """
    while True:
        try:
            consumer = KafkaConsumer(
                'weather-api',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id=None  # tanpa commit offset: selalu replay dari awal tiap restart
            )

            print("[Dashboard] Weather API Consumer started...")

            for message in consumer:
                data = message.value
                kode_kota = data.get('kode_kota')

                if kode_kota:
                    latest_weather[kode_kota] = data
                    weather_history[kode_kota].append(data)
                    print(f"[Dashboard] Updated {kode_kota}: {data['temperature']}C")

        except Exception as e:
            print(f"[Dashboard] Error in weather consumer: {e}, reconnect dalam 10s")
            time.sleep(10)

def consume_weather_rss():
    """
    Background thread: Consumer untuk topic 'weather-rss'
    Update latest_news dengan artikel terbaru
    """
    while True:
        try:
            consumer = KafkaConsumer(
                'weather-rss',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id=None  # tanpa commit offset: selalu replay dari awal tiap restart
            )

            print("[Dashboard] Weather RSS Consumer started...")

            for message in consumer:
                artikel = message.value
                latest_news.appendleft(artikel)
                print(f"[Dashboard] New article: {artikel['judul'][:50]}...")

        except Exception as e:
            print(f"[Dashboard] Error in RSS consumer: {e}, reconnect dalam 10s")
            time.sleep(10)

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
    Load Spark analysis results dari JSON file
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

# ============================================================================
# GOLD LAYER HELPER FUNCTIONS
# ============================================================================

def read_gold_table(table_name):
    """
    Baca table dari Gold layer (Parquet format)
    Returns DataFrame atau None jika error
    """
    if not spark_session:
        return None

    try:
        table_path = f"{GOLD_PATH}/{table_name}"
        if os.path.exists(table_path):
            # Try Parquet first (current format), fallback to Delta if available
            try:
                df = spark_session.read.format("parquet").load(table_path)
            except:
                df = spark_session.read.format("delta").load(table_path)
            return df
    except Exception as e:
        print(f"[Dashboard] ⚠️ Error reading Gold table {table_name}: {e}")

    return None

def get_weather_analytics():
    """
    Get time-series weather analytics dari Gold layer
    Returns: List of dicts dengan temp_current, temp_change, temp_ma3, humidity, wind_speed
    """
    df = read_gold_table("weather_analytics")
    if df is None:
        return None

    try:
        data = df.select(
            "kode_kota", "nama_kota", "temp_current", "temp_change_1h",
            "temp_moving_avg_3h", "humidity", "wind_speed", "reading_time"
        ).collect()

        return [row.asDict() for row in data]
    except Exception as e:
        print(f"[Dashboard] ⚠️ Error in get_weather_analytics: {e}")

    return None

def get_weather_extremes():
    """
    Get weather extremes summary dari Gold layer
    Returns: List of dicts dengan city stats (max_temp, min_temp, avg_temp, etc)
    """
    df = read_gold_table("weather_extremes")
    if df is None:
        return None

    try:
        data = df.select(
            "kode_kota", "nama_kota", "max_temp", "min_temp", "avg_temp",
            "max_humidity", "max_wind", "reading_count", "temp_range"
        ).collect()

        return [row.asDict() for row in data]
    except Exception as e:
        print(f"[Dashboard] ⚠️ Error in get_weather_extremes: {e}")

    return None

def get_news_analytics():
    """
    Get news source distribution dari Gold layer
    Returns: List of dicts dengan sumber, article_count, latest_article
    """
    df = read_gold_table("news_by_source")
    if df is None:
        return None

    try:
        data = df.select(
            "sumber", "article_count", "latest_article"
        ).collect()

        return [row.asDict() for row in data]
    except Exception as e:
        print(f"[Dashboard] ⚠️ Error in get_news_analytics: {e}")

    return None

def get_recent_news_gold():
    """
    Get top 20 recent news dari Gold layer
    Returns: List of dicts dengan judul, sumber, waktu_terbit, ringkasan
    """
    df = read_gold_table("recent_news")
    if df is None:
        return None

    try:
        data = df.select(
            "news_rank", "judul", "sumber", "waktu_terbit", "ringkasan"
        ).collect()

        return [row.asDict() for row in data]
    except Exception as e:
        print(f"[Dashboard] ⚠️ Error in get_recent_news_gold: {e}")

    return None

def get_weather_news_correlation():
    """
    Get weather-news correlation analysis dari Gold layer
    Returns: List of dicts dengan hour, avg_temp, max_humidity, max_wind, news_count
    """
    df = read_gold_table("weather_news_correlation")
    if df is None:
        return None

    try:
        # Select and order by time
        data = df.select(
            "kode_kota", "nama_kota", "hour", "avg_temp",
            "max_humidity", "max_wind", "news_count"
        ).orderBy("hour").collect()

        return [row.asDict() for row in data]
    except Exception as e:
        print(f"[Dashboard] ⚠️ Error in get_weather_news_correlation: {e}")

    return None

def is_extreme_weather(temp, humidity, wind_speed):
    """
    Check if weather is extreme condition
    Criteria: wind_speed > 40 OR humidity > 90 OR temperature > 35
    """
    return wind_speed > 40 or humidity > 90 or temp > 35

# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def index():
    """Render dashboard homepage"""
    return render_template('index.html')

@app.route('/api/current_weather')
def api_current_weather():
    """
    Return latest weather data untuk semua kota
    Format: 
    {
        "JKT": {
            "nama_kota": "Jakarta",
            "temperature": 28.5,
            "humidity": 75,
            "wind_speed": 10,
            "weather_code": 0,
            "timestamp": "2026-05-06 14:30:45",
            "temp_color": "normal",
            "weather_desc": "Cerah",
            "is_extreme": false
        },
        ...
    }
    """
    result = {}
    
    for kode, info in CITIES_INFO.items():
        if kode in latest_weather:
            data = latest_weather[kode]
            temp = data.get('temperature', 0)
            humidity = data.get('humidity', 0)
            wind = data.get('wind_speed', 0)
            
            result[kode] = {
                'kode_kota': kode,
                'nama_kota': data.get('nama_kota', info['nama']),
                'temperature': temp,
                'humidity': humidity,
                'wind_speed': wind,
                'weather_code': data.get('weather_code', -1),
                'weather_desc': get_weather_description(data.get('weather_code', -1)),
                'timestamp': data.get('timestamp', 'N/A'),
                'temp_color': get_temperature_color(temp),
                'is_extreme': is_extreme_weather(temp, humidity, wind),
                'latitude': info['lat'],
                'longitude': info['lon']
            }
        else:
            # Placeholder untuk kota yang belum ada data
            result[kode] = {
                'kode_kota': kode,
                'nama_kota': info['nama'],
                'temperature': None,
                'humidity': None,
                'wind_speed': None,
                'weather_code': -1,
                'weather_desc': 'Menunggu data...',
                'timestamp': 'N/A',
                'temp_color': 'pending',
                'is_extreme': False,
                'latitude': info['lat'],
                'longitude': info['lon']
            }
    
    return jsonify(result)

@app.route('/api/extreme_weather')
def api_extreme_weather():
    """
    Return list kota dengan kondisi ekstrem
    Format: {
        "extreme_cities": [
            {
                "kode": "JKT",
                "nama": "Jakarta",
                "temperature": 36.5,
                "humidity": 92,
                "wind_speed": 45,
                "reason": ["temp > 35°C", "humidity > 90%", "wind > 40 km/h"]
            }
        ]
    }
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
    Return Spark analysis results
    3 analisis wajib:
    1. Perbandingan suhu antar kota
    2. Deteksi kondisi ekstrem
    3. Tren suhu per jam
    """
    results = load_spark_results()
    
    if results:
        return jsonify({
            'status': 'success',
            'data': results,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Spark analysis belum tersedia',
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

@app.route('/api/latest_news')
def api_latest_news():
    """
    Return latest news dari RSS feeds
    Format: {
        "news": [
            {
                "judul": "...",
                "link": "...",
                "ringkasan": "...",
                "sumber": "...",
                "waktu_terbit": "..."
            }
        ]
    }
    """
    news_list = list(latest_news)[:10]  # Return top 10 latest
    
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
        'gold_layer_available': spark_session is not None
    })

# ============================================================================
# GOLD LAYER API ENDPOINTS
# ============================================================================

@app.route('/api/gold/weather_analytics')
def api_gold_weather_analytics():
    """
    Get weather time-series analytics dari Gold layer
    Menampilkan temp trends, moving averages per kota
    """
    if not spark_session:
        return jsonify({
            'status': 'error',
            'message': 'Spark session not available',
            'data': None
        }), 503

    data = get_weather_analytics()

    if data:
        # Convert Timestamp objects to string
        for row in data:
            if 'reading_time' in row and row['reading_time']:
                row['reading_time'] = str(row['reading_time'])

        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Gold layer weather_analytics table tidak tersedia',
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

@app.route('/api/gold/weather_extremes')
def api_gold_weather_extremes():
    """
    Get weather extremes summary per kota
    Menampilkan max_temp, min_temp, avg_temp, temp_range
    """
    if not spark_session:
        return jsonify({
            'status': 'error',
            'message': 'Spark session not available',
            'data': None
        }), 503

    data = get_weather_extremes()

    if data:
        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Gold layer weather_extremes table tidak tersedia',
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

@app.route('/api/gold/news_analytics')
def api_gold_news_analytics():
    """
    Get news source distribution analytics
    Menampilkan artikel count per source, latest article time
    """
    if not spark_session:
        return jsonify({
            'status': 'error',
            'message': 'Spark session not available',
            'data': None
        }), 503

    data = get_news_analytics()

    if data:
        # Convert timestamp to string
        for row in data:
            if 'latest_article' in row and row['latest_article']:
                row['latest_article'] = str(row['latest_article'])

        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Gold layer news_by_source table tidak tersedia',
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

@app.route('/api/gold/recent_news')
def api_gold_recent_news():
    """
    Get top 20 recent news dari Gold layer
    Includes ranking, judul, sumber, waktu_terbit, ringkasan
    """
    if not spark_session:
        return jsonify({
            'status': 'error',
            'message': 'Spark session not available',
            'data': None
        }), 503

    data = get_recent_news_gold()

    if data:
        # Convert timestamp to string
        for row in data:
            if 'waktu_terbit' in row and row['waktu_terbit']:
                row['waktu_terbit'] = str(row['waktu_terbit'])

        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Gold layer recent_news table tidak tersedia',
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

@app.route('/api/gold/weather_news_correlation')
def api_gold_weather_news_correlation():
    """
    Get weather-news correlation analysis
    Menampilkan bagaimana weather events berkorelasi dengan news volume
    """
    if not spark_session:
        return jsonify({
            'status': 'error',
            'message': 'Spark session not available',
            'data': None
        }), 503

    data = get_weather_news_correlation()

    if data:
        # Convert hour field (it's a Row from window function)
        processed_data = []
        for row in data:
            row_dict = row.copy() if isinstance(row, dict) else row.asDict()
            if 'hour' in row_dict and row_dict['hour']:
                # hour adalah StructType dengan start dan end
                try:
                    hour_val = row_dict['hour']
                    if hasattr(hour_val, 'start'):
                        row_dict['hour_start'] = str(hour_val.start)
                        row_dict['hour_end'] = str(hour_val.end)
                    else:
                        row_dict['hour_start'] = str(hour_val)
                except:
                    pass
            processed_data.append(row_dict)

        return jsonify({
            'status': 'success',
            'data': processed_data,
            'count': len(processed_data),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        return jsonify({
            'status': 'pending',
            'message': 'Gold layer weather_news_correlation table tidak tersedia',
            'data': None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 202

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
    print("⛅ WeatherPulse Dashboard Server - with Medallion Integration")
    print("="*70)
    print(f"✨ Server starting at http://localhost:5000")
    print(f"📊 Kafka Consumer Endpoints (Real-time):")
    print(f"   - GET /api/current_weather")
    print(f"   - GET /api/extreme_weather")
    print(f"   - GET /api/latest_news")
    print(f"🥇 Gold Layer Analytics Endpoints:")
    print(f"   - GET /api/gold/weather_analytics (time-series analysis)")
    print(f"   - GET /api/gold/weather_extremes (temperature ranges)")
    print(f"   - GET /api/gold/news_analytics (source distribution)")
    print(f"   - GET /api/gold/recent_news (top 20 articles)")
    print(f"   - GET /api/gold/weather_news_correlation (weather-news correlation)")
    print(f"🔧 Status Endpoints:")
    print(f"   - GET /api/health")
    print("="*70 + "\n")

    if spark_session:
        print("✅ Spark session ready - Gold layer analytics enabled\n")
    else:
        print("⚠️  Spark session unavailable - Gold layer analytics disabled\n")

    # Run Flask server
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,  # Set True hanya untuk development
        use_reloader=False  # Important untuk background threads
    )
