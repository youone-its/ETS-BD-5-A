"""
Weather RSS Producer - Ambil berita cuaca dari RSS dan kirim ke Kafka.
"""

import threading
import time
import hashlib
import feedparser
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import (
    BOOTSTRAP_SERVERS, RSS_URLS, TOPIC_RSS, INTERVAL_RSS
)


feedparser.USER_AGENT = "Mozilla/5.0 (compatible; WeatherPulse/1.0)"


def create_topic_if_not_exists(topic_name: str):
    """Buat topik Kafka jika belum ada."""
    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id='rss_admin',
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


def hash_url(url: str) -> str:
    """Hash URL untuk key Kafka."""
    return hashlib.md5(url.encode()).hexdigest()[:8]


def run_producer(stop_flag: threading.Event):
    """Jalankan producer RSS dalam loop."""
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

    sudah_dikirim = set()

    print("[RSS Producer] Dimulai")
    while not stop_flag.is_set():
        print(f"\n[RSS Producer] [{datetime.now().strftime('%H:%M:%S')}] Polling...")
        total_baru = 0

        for url in RSS_URLS:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    link = entry.get("link", "")
                    if not link or link in sudah_dikirim:
                        continue

                    artikel = {
                        "judul": entry.get("title", ""),
                        "link": link,
                        "ringkasan": entry.get("summary", "")[:300],
                        "waktu_terbit": entry.get("published", datetime.now().isoformat()),
                        "sumber": feed.feed.get("title", url),
                        "timestamp": datetime.now().isoformat(),
                    }
                    key = hash_url(link)
                    producer.send(TOPIC_RSS, key=key, value=artikel)
                    sudah_dikirim.add(link)
                    total_baru += 1
            except Exception as e:
                print(f"  [ERROR] RSS {url}: {e}")

        producer.flush()
        print(f"  {total_baru} artikel baru dikirim ke {TOPIC_RSS}")

        for _ in range(INTERVAL_RSS // 5):
            if stop_flag.is_set():
                break
            time.sleep(5)

    print("[RSS Producer] Berhenti")
    producer.close()


if __name__ == "__main__":
    create_topic_if_not_exists(TOPIC_RSS)

    stop_flag = threading.Event()
    thread = threading.Thread(target=run_producer, args=(stop_flag,), daemon=False)
    thread.start()

    try:
        thread.join()
    except KeyboardInterrupt:
        print("\n[RSS Producer] Menghentikan...")
        stop_flag.set()
        thread.join(timeout=5)
