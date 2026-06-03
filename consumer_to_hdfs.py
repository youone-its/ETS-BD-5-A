"""
Kafka Consumer → Local Storage (simulating HDFS).
Karena Docker-in-Docker tidak tersedia, simpan ke local volume yang dimount.
"""

import os
import json
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer

from config import (
    BOOTSTRAP_SERVERS, TOPIC_API, TOPIC_RSS,
    HDFS_API_PATH, HDFS_RSS_PATH, FLUSH_INTERVAL
)


def create_local_directory(path: str):
    """Pastikan direktori local sudah ada (simulating HDFS)."""
    os.makedirs(path, exist_ok=True)
    print(f"✅ {path} Siap.")


def save_to_local(data: list, local_path: str, label: str):
    """Simpan data ke filesystem local (simulating HDFS)."""
    if not data:
        return

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{local_path}/{label.lower()}_{ts}.json"

    with open(filename, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  [{label}] ✅ {filename} ({len(data)} event)")


def run_consumer(topic: str, local_path: str, label: str, stop_flag: threading.Event):
    """Jalankan consumer untuk topic tertentu."""
    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=f"consumer-{label.lower()}",
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=2000,
                api_version=(0, 11, 0),
            )
            break
        except Exception as e:
            retry_count += 1
            wait_time = 5 * retry_count
            print(f"[Consumer {label}] Kafka belum siap, retry dalam {wait_time}s... ({retry_count}/{max_retries})")
            time.sleep(wait_time)
            if retry_count >= max_retries:
                print(f"[Consumer {label}] Gagal connect ke Kafka setelah {max_retries} kali")
                return

    buffer = []
    lock = threading.Lock()

    print(f"[Consumer {label}] Dimulai, topic: {topic}")
    last_flush = time.time()

    while not stop_flag.is_set():
        try:
            for msg in consumer:
                with lock:
                    buffer.append(msg.value)
                if stop_flag.is_set():
                    break

            if time.time() - last_flush >= FLUSH_INTERVAL:
                with lock:
                    data_copy = buffer.copy()
                    buffer.clear()
                save_to_local(data_copy, local_path, label)
                last_flush = time.time()
        except Exception as e:
            print(f"[Consumer {label}] Error: {e}")
            time.sleep(5)

    with lock:
        data_copy = buffer.copy()
        buffer.clear()
    save_to_local(data_copy, local_path, label)

    consumer.close()
    print(f"[Consumer {label}] Berhenti")


if __name__ == "__main__":
    # Create local directories (simulating HDFS)
    os.makedirs("/data/weather/api", exist_ok=True)
    os.makedirs("/data/weather/rss", exist_ok=True)

    create_local_directory("/data/weather/api")
    create_local_directory("/data/weather/rss")

    stop_flag = threading.Event()

    thread_api = threading.Thread(
        target=run_consumer,
        args=(TOPIC_API, "/data/weather/api", "API", stop_flag),
        daemon=False
    )
    thread_rss = threading.Thread(
        target=run_consumer,
        args=(TOPIC_RSS, "/data/weather/rss", "RSS", stop_flag),
        daemon=False
    )

    thread_api.start()
    thread_rss.start()

    try:
        thread_api.join()
        thread_rss.join()
    except KeyboardInterrupt:
        print("\n[Consumer] Menghentikan...")
        stop_flag.set()
        thread_api.join(timeout=5)
        thread_rss.join(timeout=5)
