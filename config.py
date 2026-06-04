"""
Konfigurasi global untuk WeatherPulse pipeline.
"""
import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092").split(",")
BOOTSTRAP_SERVERS = [s.strip() for s in BOOTSTRAP_SERVERS]

KOTA = [
    {"kode": "JKT", "nama": "Jakarta",  "lat": -6.21, "lon": 106.85},
    {"kode": "SBY", "nama": "Surabaya", "lat": -7.25, "lon": 112.75},
    {"kode": "SMG", "nama": "Semarang", "lat": -6.99, "lon": 110.42},
    {"kode": "MDN", "nama": "Medan",    "lat": -3.59, "lon": 98.67},
    {"kode": "MKS", "nama": "Makassar", "lat": -5.14, "lon": 119.41},
    {"kode": "DPS", "nama": "Denpasar", "lat": -8.67, "lon": 115.21},
]

RSS_URLS = [
    "https://www.antaranews.com/rss/warta-bumi.xml",
    "https://www.mongabay.co.id/feed/",
]

TOPIC_API = "weather-api"
TOPIC_RSS = "weather-rss"

INTERVAL_API = 60   # poll tiap 60 detik
INTERVAL_RSS = 60   # poll tiap 60 detik

HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "8020")
HDFS_URI = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"

HDFS_API_PATH = "/data/weather/api"
HDFS_RSS_PATH = "/data/weather/rss"

FLUSH_INTERVAL = 30
