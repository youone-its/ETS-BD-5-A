import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, 
    trim, lower, upper, coalesce, when, round,
    row_number
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("medallion-silver") \
    .getOrCreate()

print("="*70)
print(" SILVER LAYER: Data Cleaning & Transformation")
print("="*70)

LAKEHOUSE_PATH = "/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver"

print("\n  Cleaning Weather API Data...")
try:
    df_weather = spark.read.format("delta").load(f"{BRONZE_PATH}/weather_api")
    print(f" Read {df_weather.count()} records from Bronze")
    
    df_weather_clean = df_weather \
        .filter(col("kode_kota").isNotNull()) \
        .filter(col("temperature").isNotNull()) \
        .filter(col("humidity").isNotNull()) \
        .filter(col("wind_speed").isNotNull())
    
    df_weather_clean = df_weather_clean \
        .withColumn("kode_kota", upper(trim(col("kode_kota")))) \
        .withColumn("nama_kota", trim(col("nama_kota"))) \
        .withColumn("temperature", round(col("temperature"), 2)) \
        .withColumn("humidity", col("humidity").cast("int")) \
        .withColumn("wind_speed", round(col("wind_speed"), 2)) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("_processed_at", current_timestamp())
    
    window_spec = Window.partitionBy("kode_kota").orderBy(col("timestamp").desc())
    df_weather_clean = df_weather_clean \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    df_weather_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/weather_api")
    print(f" Written {df_weather_clean.count()} cleaned weather records to Silver")
    
except Exception as e:
    print(f"  Error processing weather data: {e}")

print("\n Cleaning News Data...")
try:
    df_news = spark.read.format("delta").load(f"{BRONZE_PATH}/weather_rss")
    print(f" Read {df_news.count()} records from Bronze")
    
    df_news_clean = df_news \
        .filter(col("judul").isNotNull()) \
        .filter(col("link").isNotNull()) \
        .filter(col("sumber").isNotNull())
    
    df_news_clean = df_news_clean \
        .withColumn("judul", trim(col("judul"))) \
        .withColumn("ringkasan", coalesce(trim(col("ringkasan")), lit(""))) \
        .withColumn("sumber", lower(trim(col("sumber")))) \
        .withColumn("waktu_terbit", to_timestamp(col("waktu_terbit"))) \
        .withColumn("_processed_at", current_timestamp())
    
    window_spec = Window.partitionBy("judul", "sumber").orderBy(col("waktu_terbit").desc())
    df_news_clean = df_news_clean \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    df_news_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/weather_rss")
    print(f" Written {df_news_clean.count()} cleaned news records to Silver")
    
except Exception as e:
    print(f"  Error processing news data: {e}")

print("\n" + "="*70)
print(" Silver layer complete!")
print(f" Transformasi: dedup, null filtering, type casting, standardisasi")
print(f" Weather API Silver: {SILVER_PATH}/weather_api")
print(f" News Silver: {SILVER_PATH}/weather_rss")
print("="*70 + "\n")

spark.stop()
