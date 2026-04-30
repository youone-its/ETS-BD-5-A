import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# [Ni’mah Fauziyyah Atok]: Konfigurasi List Kota sesuai ketentuan Topik 2
CITIES = [
    {"kode": "JKT", "nama": "Jakarta", "lat": -6.21, "lon": 106.85},
    {"kode": "SBY", "nama": "Surabaya", "lat": -7.25, "lon": 112.75},
    {"kode": "SMG", "nama": "Semarang", "lat": -6.99, "lon": 110.42},
    {"kode": "MDN", "nama": "Medan", "lat": -3.59, "lon": 98.67},
    {"kode": "MKS", "nama": "Makassar", "lat": -5.14, "lon": 119.41},
    {"kode": "DPS", "nama": "Denpasar", "lat": -8.67, "lon": 115.21}
]

TOPIC_NAME = "weather-api"
# Interval polling 10 menit (600 detik) sesuai ketentuan
POLLING_INTERVAL = 600 

def get_weather_data(lat, lon):
    """
    [Ni’mah Fauziyyah Atok]: Mengambil data cuaca real-time dari Open-Meteo API.
    Tanpa API Key sesuai petunjuk ETS.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "Asia/Jakarta"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def run_producer():
    """
    [Ni’mah Fauziyyah Atok]: Inisialisasi Kafka Producer dengan konfigurasi
    Idempotence dan Acks='all' untuk menjamin pengiriman data.
    """
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        # Serialize data ke JSON
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # Key berdasarkan kode kota (JKT, SBY, dll)
        key_serializer=lambda x: x.encode('utf-8'),
        enable_idempotence=True,
        acks='all'
    )

    print(f"Starting Producer for topic: {TOPIC_NAME}...")

    try:
        while True:
            for city in CITIES:
                raw_data = get_weather_data(city["lat"], city["lon"])
                
                if raw_data and "current" in raw_data:
                    current = raw_data["current"]
                    
                    # [Ni’mah Fauziyyah Atok]: Standarisasi format JSON agar mudah diproses Spark
                    payload = {
                        "kode_kota": city["kode"],
                        "nama_kota": city["nama"],
                        "temperature": current["temperature_2m"],
                        "humidity": current["relative_humidity_2m"],
                        "wind_speed": current["wind_speed_10m"],
                        "weather_code": current["weather_code"],
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    # Kirim ke Kafka
                    producer.send(
                        TOPIC_NAME, 
                        key=city["kode"], 
                        value=payload
                    )
                    print(f"[{payload['timestamp']}] Sent: {city['kode']} - {payload['temperature']}°C")

            # Flush untuk memastikan data terkirim
            producer.flush()
            print(f"Waiting for {POLLING_INTERVAL} seconds...")
            time.sleep(POLLING_INTERVAL)

    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
