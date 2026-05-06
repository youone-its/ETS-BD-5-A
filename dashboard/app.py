"""
WeatherPulse Dashboard - Flask Backend
Balqis Sani Sabillah - 5027241002

Features:
- Real-time weather data dari Kafka (weather-api topic)
- Latest news dari Kafka (weather-rss topic)  
- Spark analysis results dari JSON
- Auto-refresh endpoints untuk frontend
"""

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

# Path untuk Spark results
SPARK_RESULTS_PATH = os.path.join(os.path.dirname(__file__), "data", "spark_results.json")

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
        'news_count': len(latest_news)
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
    print(f"   - GET /api/current_weather")
    print(f"   - GET /api/extreme_weather")
    print(f"   - GET /api/spark_results")
    print(f"   - GET /api/latest_news")
    print(f"   - GET /api/health")
    print("="*70 + "\n")
    
    # Run Flask server
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,  # Set True hanya untuk development
        use_reloader=False  # Important untuk background threads
    )
