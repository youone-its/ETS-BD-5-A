import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    trim, lower, upper, coalesce, round,
    row_number
)
from pyspark.sql.window import Window

LAKEHOUSE_PATH = "/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver"


def build_spark():
    return SparkSession.builder \
        .appName("medallion-silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


class SilverTransform:
    def __init__(self, spark):
        self.spark = spark

    def weather_api(self):
        df = self.spark.read.format("delta").load(f"{BRONZE_PATH}/weather_api")
        print(f"Read {df.count()} records dari Bronze weather_api")

        df = df \
            .filter(col("kode_kota").isNotNull()) \
            .filter(col("temperature").isNotNull()) \
            .filter(col("humidity").isNotNull()) \
            .filter(col("wind_speed").isNotNull()) \
            .withColumn("kode_kota", upper(trim(col("kode_kota")))) \
            .withColumn("nama_kota", trim(col("nama_kota"))) \
            .withColumn("temperature", round(col("temperature"), 2)) \
            .withColumn("humidity", col("humidity").cast("int")) \
            .withColumn("wind_speed", round(col("wind_speed"), 2)) \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("_processed_at", current_timestamp())

        window = Window.partitionBy("kode_kota").orderBy(col("timestamp").desc())
        df = df.withColumn("_rn", row_number().over(window)) \
               .filter(col("_rn") == 1) \
               .drop("_rn")

        df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/weather_api")
        print(f"Silver weather_api: {df.count()} records")

    def weather_rss(self):
        df = self.spark.read.format("delta").load(f"{BRONZE_PATH}/weather_rss")
        print(f"Read {df.count()} records dari Bronze weather_rss")

        df = df \
            .filter(col("judul").isNotNull()) \
            .filter(col("link").isNotNull()) \
            .filter(col("sumber").isNotNull()) \
            .withColumn("judul", trim(col("judul"))) \
            .withColumn("ringkasan", coalesce(trim(col("ringkasan")), lit(""))) \
            .withColumn("sumber", lower(trim(col("sumber")))) \
            .withColumn("waktu_terbit", to_timestamp(col("waktu_terbit"))) \
            .withColumn("_processed_at", current_timestamp())

        window = Window.partitionBy("judul", "sumber").orderBy(col("waktu_terbit").desc())
        df = df.withColumn("_rn", row_number().over(window)) \
               .filter(col("_rn") == 1) \
               .drop("_rn")

        df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/weather_rss")
        print(f"Silver weather_rss: {df.count()} records")


def main():
    print("=" * 70)
    print("SILVER LAYER: Data Cleaning & Transformation")
    print("=" * 70)

    spark = build_spark()
    t = SilverTransform(spark)
    t.weather_api()
    t.weather_rss()
    spark.stop()

    print("=" * 70)
    print("Silver layer complete")
    print(f"Output: {SILVER_PATH}/weather_api, {SILVER_PATH}/weather_rss")
    print("=" * 70)


if __name__ == "__main__":
    main()
