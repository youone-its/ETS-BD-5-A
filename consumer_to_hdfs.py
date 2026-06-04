import json
import threading
import time
from datetime import datetime

import requests
from kafka import KafkaConsumer

from config import (
    BOOTSTRAP_SERVERS, TOPIC_API, TOPIC_RSS,
    HDFS_API_PATH, HDFS_RSS_PATH, FLUSH_INTERVAL,
    HDFS_HOST
)

WEBHDFS = f"http://{HDFS_HOST}:9870/webhdfs/v1"
HDFS_USER = "hadoop"


def hdfs_mkdir(path):
    r = requests.put(f"{WEBHDFS}{path}?op=MKDIRS&user.name={HDFS_USER}")
    r.raise_for_status()
    print(f"{path} Siap.")


def hdfs_write(hdfs_path, filename, data):
    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    url = f"{WEBHDFS}{hdfs_path}/{filename}?op=CREATE&overwrite=true&user.name={HDFS_USER}"

    # Step 1: namenode returns 307 redirect ke datanode
    r1 = requests.put(url, allow_redirects=False)
    if r1.status_code != 307:
        r1.raise_for_status()

    datanode_url = r1.headers["Location"]

    # Step 2: tulis content ke datanode
    r2 = requests.put(datanode_url, data=content, headers={"Content-Type": "application/octet-stream"})
    r2.raise_for_status()
    print(f"  [{filename}] HDFS {hdfs_path} ({len(data)} record)")


def run_consumer(topic, hdfs_path, label, stop_flag):
    for attempt in range(1, 11):
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
            wait = 5 * attempt
            print(f"[Consumer {label}] Kafka belum siap, retry {attempt}/10 dalam {wait}s")
            time.sleep(wait)
            if attempt >= 10:
                print(f"[Consumer {label}] Gagal connect ke Kafka")
                return

    buffer = []
    lock = threading.Lock()
    last_flush = time.time()

    print(f"[Consumer {label}] Dimulai, topic: {topic}")

    while not stop_flag.is_set():
        try:
            for msg in consumer:
                with lock:
                    buffer.append(msg.value)
                if stop_flag.is_set():
                    break

            if time.time() - last_flush >= FLUSH_INTERVAL:
                with lock:
                    batch = buffer.copy()
                    buffer.clear()
                if batch:
                    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    hdfs_write(hdfs_path, f"{label.lower()}_{ts}.json", batch)
                last_flush = time.time()

        except Exception as e:
            print(f"[Consumer {label}] Error: {e}")
            time.sleep(5)

    with lock:
        batch = buffer.copy()
        buffer.clear()
    if batch:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        hdfs_write(hdfs_path, f"{label.lower()}_{ts}.json", batch)

    consumer.close()
    print(f"[Consumer {label}] Berhenti")


if __name__ == "__main__":
    # tunggu namenode WebHDFS siap
    for attempt in range(1, 13):
        try:
            r = requests.get(f"{WEBHDFS}/?op=LISTSTATUS&user.name={HDFS_USER}", timeout=5)
            if r.status_code in (200, 404):
                break
        except Exception:
            pass
        print(f"Menunggu HDFS WebHDFS siap... ({attempt}/12)")
        time.sleep(10)

    hdfs_mkdir(HDFS_API_PATH)
    hdfs_mkdir(HDFS_RSS_PATH)

    stop_flag = threading.Event()

    thread_api = threading.Thread(
        target=run_consumer,
        args=(TOPIC_API, HDFS_API_PATH, "API", stop_flag),
        daemon=False,
    )
    thread_rss = threading.Thread(
        target=run_consumer,
        args=(TOPIC_RSS, HDFS_RSS_PATH, "RSS", stop_flag),
        daemon=False,
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
