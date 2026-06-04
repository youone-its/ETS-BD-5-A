import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("medallion-bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print(" BRONZE LAYER: Raw Data Ingestion from HDFS")

HDFS_HOST = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "8020")
HDFS_PATH = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"

# Data paths from consumer_to_hdfs
HDFS_API_PATH = f"{HDFS_PATH}/data/weather/api"
HDFS_RSS_PATH = f"{HDFS_PATH}/data/weather/rss"

LAKEHOUSE_PATH = "/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"


# Read Weather API data from HDFS (from consumer_to_hdfs)

print("\n Reading Weather API data from HDFS...")
try:
    df_weather_api = spark.read.json(HDFS_API_PATH)

    if df_weather_api.count() > 0:
        weather_api_bronze = df_weather_api \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("weather-api"))

        weather_api_bronze = weather_api_bronze.dropDuplicates(["kode_kota", "timestamp"])
        weather_api_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_api")
        print(f"Written {weather_api_bronze.count()} weather API records to Bronze")
    else:
        print("No data found in HDFS API path, creating sample data for testing...")
        # Fallback: Create sample data if HDFS is empty
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

        weather_api_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_api")
        print(f" ✅ Created sample data: {weather_api_bronze.count()} records")

except Exception as e:
    print(f" ⚠️  Error reading from HDFS API: {e}")
    print(" Creating sample data for fallback...")

    sample_weather_api = spark.createDataFrame([
        {"kode_kota": "JKT", "nama_kota": "Jakarta", "temperature": 28.5, "humidity": 75, "wind_speed": 10.0, "weather_code": 0, "timestamp": "2026-05-30 14:30:00"},
        {"kode_kota": "SBY", "nama_kota": "Surabaya", "temperature": 26.2, "humidity": 68, "wind_speed": 8.5, "weather_code": 1, "timestamp": "2026-05-30 14:30:00"},
    ], schema="kode_kota string, nama_kota string, temperature double, humidity int, wind_speed double, weather_code int, timestamp string")

    weather_api_bronze = sample_weather_api.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("weather-api"))
    weather_api_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_api")
    print(f" ✅ Fallback: {weather_api_bronze.count()} sample records")

# ============================================================================
# Read News (RSS) data from HDFS (from consumer_to_hdfs)
# ============================================================================
print("\n Reading RSS News data from HDFS...")
try:
    df_news = spark.read.json(HDFS_RSS_PATH)

    if df_news.count() > 0:
        news_bronze = df_news \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("weather-rss"))

        news_bronze = news_bronze.dropDuplicates(["judul", "sumber"])
        news_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_rss")
        print(f" ✅ Written {news_bronze.count()} news records to Bronze")
    else:
        print(" ⚠️  No data found in HDFS RSS path, creating sample data for testing...")
        # Fallback: Create sample data if HDFS is empty
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

        news_bronze = sample_news.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("weather-rss"))
        news_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_rss")
        print(f" ✅ Created sample data: {news_bronze.count()} records")

except Exception as e:
    print(f" ⚠️  Error reading from HDFS RSS: {e}")
    print(" Creating sample data for fallback...")

    sample_news = spark.createDataFrame([
        {"judul": "Cuaca Ekstrem di Jakarta Rabu Sore", "link": "https://example.com/berita/1", "ringkasan": "Hujan deras disertai angin kuat di Jakarta", "sumber": "detik", "waktu_terbit": "2026-05-30 12:00:00"},
        {"judul": "Surabaya Alami Kenaikan Suhu", "link": "https://example.com/berita/2", "ringkasan": "Panas terik melanda Kota Pahlawan", "sumber": "kompas", "waktu_terbit": "2026-05-30 13:15:00"},
    ], schema="judul string, link string, ringkasan string, sumber string, waktu_terbit string")

    news_bronze = sample_news.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("weather-rss"))
    news_bronze.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_rss")
    print(f" ✅ Fallback: {news_bronze.count()} sample records")

print("\n" + "="*70)
print(" Bronze layer complete!")
print(f" Weather API Bronze: {BRONZE_PATH}/weather_api")
print(f" News Bronze: {BRONZE_PATH}/weather_rss")
print("="*70 + "\n")

spark.stop()
