import json
import time
import hashlib
import feedparser
from datetime import datetime
from kafka import KafkaProducer

# [Ni’mah Fauziyyah Atok]: Konfigurasi diambil dari source: 3
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_RSS = "weather-rss"
INTERVAL_RSS = 300  # 5 menit sesuai ketentuan ETS

RSS_URLS = [
    "https://www.antaranews.com/rss/warta-bumi.xml",
    "https://www.mongabay.co.id/feed/",
]

# Deduplication set agar tidak mengirim artikel yang sama berulang kali
sudah_dikirim = set()

def hash_url(url: str) -> str:
    """[Ni’mah Fauziyyah Atok]: Membuat key unik dari URL artikel"""
    return hashlib.md5(url.encode()).hexdigest()[:8]

def run_rss_producer():
    """[Ni’mah Fauziyyah Atok]: Fungsi utama producer RSS"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        enable_idempotence=True,
        acks="all"
    )

    print(f"[RSS Producer] Dimulai untuk topic: {TOPIC_RSS}")
    
    try:
        while True:
            print(f"\n[RSS Producer] [{datetime.now().strftime('%H:%M:%S')}] Polling RSS...")
            total_baru = 0
            
            for url in RSS_URLS:
                try:
                    feed = feedparser.parse(url)
                    for entry in feed.entries:
                        link = entry.get("link", "")
                        
                        # Cek apakah artikel sudah pernah dikirim
                        if not link or link in sudah_dikirim:
                            continue
                            
                        artikel = {
                            "judul":        entry.get("title", ""),
                            "link":         link,
                            "ringkasan":    entry.get("summary", "")[:300],
                            "waktu_terbit": entry.get("published", datetime.now().isoformat()),
                            "sumber":       feed.feed.get("title", url),
                            "timestamp":    datetime.now().isoformat(),
                        }
                        
                        key = hash_url(link)
                        producer.send(TOPIC_RSS, key=key, value=artikel)
                        sudah_dikirim.add(link)
                        total_baru += 1
                except Exception as e:
                    print(f"  [ERROR] RSS {url}: {e}")
            
            producer.flush()
            print(f"  {total_baru} artikel baru dikirim ke {TOPIC_RSS}")
            print(f"Waiting for {INTERVAL_RSS} seconds...")
            time.sleep(INTERVAL_RSS)

    except KeyboardInterrupt:
        print("[RSS Producer] Berhenti")
    finally:
        producer.close()

if __name__ == "__main__":
    run_rss_producer()