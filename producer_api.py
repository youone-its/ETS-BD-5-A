"""
Weather API Producer - Ambil data cuaca dari Open-Meteo dan kirim ke Kafka.
"""

import threading
import time
import requests
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import (
    BOOTSTRAP_SERVERS, KOTA, TOPIC_API, INTERVAL_API
)


def create_topic_if_not_exists(topic_name: str):
    """Buat topik Kafka jika belum ada."""
    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id='weather_admin',
                api_version=(0, 11, 0),
            )
            topik_baru = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[topik_baru], validate_only=False)
            print(f"✅ Topik '{topic_name}' berhasil dibuat.")
            admin_client.close()
            return
        except TopicAlreadyExistsError:
            print(f"ℹ️ Topik '{topic_name}' sudah ada.")
            admin_client.close()
            return
        except Exception as e:
            retry_count += 1
            wait_time = 5 * retry_count
            print(f"⏳ Kafka belum siap, retry dalam {wait_time}s... ({retry_count}/{max_retries})")
            time.sleep(wait_time)
            if retry_count >= max_retries:
                print(f"❌ Gagal membuat topik '{topic_name}' setelah {max_retries} kali")
                return


def fetch_weather(kota: dict) -> dict | None:
    """Ambil data cuaca dari Open-Meteo API."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": kota["lat"],
        "longitude": kota["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "Asia/Jakarta",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()["current"]
        return {
            "kode_kota": kota["kode"],
            "nama_kota": kota["nama"],
            "temperature": data["temperature_2m"],
            "humidity": data["relative_humidity_2m"],
            "wind_speed": data["wind_speed_10m"],
            "weather_code": data["weather_code"],
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"  [ERROR] {kota['nama']}: {e}")
        return None


def run_producer(stop_flag: threading.Event):
    """Jalankan producer API dalam loop."""
    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                enable_idempotence=True,
                acks="all",
                api_version=(0, 11, 0),
            )
            break
        except Exception as e:
            retry_count += 1
            wait_time = 5 * retry_count
            print(f"⏳ Kafka belum siap, retry dalam {wait_time}s... ({retry_count}/{max_retries})")
            time.sleep(wait_time)
            if retry_count >= max_retries:
                print(f"❌ Gagal connect ke Kafka setelah {max_retries} kali")
                return

    print("[API Producer] Dimulai")
    while not stop_flag.is_set():
        print(f"\n[API Producer] [{datetime.now().strftime('%H:%M:%S')}] Polling...")
        for kota in KOTA:
            if stop_flag.is_set():
                break
            event = fetch_weather(kota)
            if event:
                producer.send(TOPIC_API, key=kota["kode"], value=event)
                print(f"   {event['kode_kota']} | {event['temperature']}°C | "
                      f" {event['wind_speed']} km/h |  {event['humidity']}%")
        producer.flush()

        for _ in range(INTERVAL_API // 5):
            if stop_flag.is_set():
                break
            time.sleep(5)

    print("[API Producer] Berhenti")
    producer.close()


if __name__ == "__main__":
    create_topic_if_not_exists(TOPIC_API)

    stop_flag = threading.Event()
    thread = threading.Thread(target=run_producer, args=(stop_flag,), daemon=False)
    thread.start()

    try:
        thread.join()
    except KeyboardInterrupt:
        print("\n[API Producer] Menghentikan...")
        stop_flag.set()
        thread.join(timeout=5)
