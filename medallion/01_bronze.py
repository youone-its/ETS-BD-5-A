import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("medallion-bronze") \
    .getOrCreate()

print("="*70)
print(" BRONZE LAYER: Raw Data Ingestion")
print("="*70)

HDFS_HOST = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "8020")
HDFS_PATH = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"

LAKEHOUSE_PATH = "/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"

print("\n Creating sample Weather API data...")
sample_weather_api = spark.createDataFrame([
    {
        "kode_kota": "JKT",
        "nama_kota": "Jakarta",
        "temperature": 28.5,
        "humidity": 75,
        "wind_speed": 10.0,
        "weather_code": 0,
        "timestamp": "2026-05-30 14:30:00",
    },
    {
        "kode_kota": "SBY",
        "nama_kota": "Surabaya",
        "temperature": 26.2,
        "humidity": 68,
        "wind_speed": 8.5,
        "weather_code": 1,
        "timestamp": "2026-05-30 14:30:00",
    },
], schema="kode_kota string, nama_kota string, temperature double, humidity int, wind_speed double, weather_code int, timestamp string")

weather_api_bronze = sample_weather_api \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source", lit("weather-api"))

weather_api_bronze = weather_api_bronze.dropDuplicates(["kode_kota", "timestamp"])

weather_api_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_api")
print(f" Written {weather_api_bronze.count()} weather API records to Bronze")

print("\n Creating sample RSS News data...")
sample_news = spark.createDataFrame([
    {
        "judul": "Cuaca Ekstrem di Jakarta Rabu Sore",
        "link": "https://example.com/berita/1",
        "ringkasan": "Hujan deras disertai angin kuat di Jakarta",
        "sumber": "detik",
        "waktu_terbit": "2026-05-30 12:00:00",
    },
    {
        "judul": "Surabaya Alami Kenaikan Suhu",
        "link": "https://example.com/berita/2",
        "ringkasan": "Panas terik melanda Kota Pahlawan",
        "sumber": "kompas",
        "waktu_terbit": "2026-05-30 13:15:00",
    },
], schema="judul string, link string, ringkasan string, sumber string, waktu_terbit string")

news_bronze = sample_news \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source", lit("weather-rss"))

news_bronze = news_bronze.dropDuplicates(["judul", "sumber"])

news_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_rss")
print(f" Written {news_bronze.count()} news records to Bronze")

print("\n" + "="*70)
print(" Bronze layer complete!")
print(f" Weather API Bronze: {BRONZE_PATH}/weather_api")
print(f" News Bronze: {BRONZE_PATH}/weather_rss")
print("="*70 + "\n")

spark.stop()
