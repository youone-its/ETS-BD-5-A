import json
import time
import os
import subprocess
import threading
from datetime import datetime
from kafka import KafkaConsumer

# [Ni’mah Fauziyyah Atok]: Konfigurasi Pipeline
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_API = "weather-api"
TOPIC_RSS = "weather-rss"

HDFS_API_PATH = "/data/weather/api"
HDFS_RSS_PATH = "/data/weather/rss"
FLUSH_INTERVAL = 120  # Simpan ke HDFS setiap 2 menit sesuai source: 3

# Buffer untuk menampung data sebelum di-flush ke HDFS
buffer_api = []
buffer_rss = []
lock_api = threading.Lock()
lock_rss = threading.Lock()

def simpan_ke_hdfs(data, hdfs_path, label):
    """[Ni’mah Fauziyyah Atok]: Logika pemindahan data dari lokal ke HDFS via Docker"""
    if not data:
        return
    
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmp_local = f"/tmp/weather_{label}_{ts}.json"
    hdfs_dest = f"{hdfs_path}/{ts}.json"

    try:
        # 1. Tulis data ke file JSON lokal sementara
        with open(tmp_local, "w") as f:
            json.dump(data, f, ensure_ascii=False)

        # 2. Copy file dari MacBook ke dalam container hadoop-namenode
        subprocess.run(["docker", "cp", tmp_local, f"hadoop-namenode:{tmp_local}"], capture_output=True)

        # 3. Jalankan perintah HDFS put di dalam container
        result = subprocess.run(
            ["docker", "exec", "hadoop-namenode", "hdfs", "dfs", "-put", "-f", tmp_local, hdfs_dest],
            capture_output=True, text=True
        )

        if result.returncode == 0:
            print(f"  [{label}] ✅ Berhasil simpan ke HDFS: {hdfs_dest} ({len(data)} event)")
        else:
            print(f"  [{label}] ❌ Gagal HDFS: {result.stderr.strip()}")

        # 4. Hapus file sampah di lokal dan di container
        os.remove(tmp_local)
        subprocess.run(["docker", "exec", "hadoop-namenode", "rm", tmp_local], capture_output=True)
        
    except Exception as e:
        print(f"  [{label}] ⚠️ Error proses HDFS: {e}")

def loop_consumer(topic, buffer, lock, hdfs_path, label):
    """[Ni’mah Fauziyyar Atok]: Thread untuk membaca Kafka dan mengisi buffer"""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"group-{label}",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000
    )
    
    print(f"[Consumer {label}] Dimulai...")
    last_flush = time.time()

    try:
        while True:
            for msg in consumer:
                with lock:
                    buffer.append(msg.value)
            
            # Cek apakah sudah waktunya flush ke HDFS
            if time.time() - last_flush >= FLUSH_INTERVAL:
                with lock:
                    data_copy = buffer.copy()
                    buffer.clear()
                
                if data_copy:
                    simpan_ke_hdfs(data_copy, hdfs_path, label)
                last_flush = time.time()
                
    except Exception as e:
        print(f"[Consumer {label}] Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Jalankan consumer API dan RSS secara paralel menggunakan threading
    t_api = threading.Thread(target=loop_consumer, args=(TOPIC_API, buffer_api, lock_api, HDFS_API_PATH, "API"))
    t_rss = threading.Thread(target=loop_consumer, args=(TOPIC_RSS, buffer_rss, lock_rss, HDFS_RSS_PATH, "RSS"))
    
    t_api.start()
    t_rss.start()
    
    t_api.join()
    t_rss.join()