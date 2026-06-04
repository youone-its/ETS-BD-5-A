import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

HDFS_HOST = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "8020")
HDFS_BASE = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"
HDFS_API_PATH = f"{HDFS_BASE}/data/weather/api"
HDFS_RSS_PATH = f"{HDFS_BASE}/data/weather/rss"

LAKEHOUSE_PATH = "/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"


def build_spark():
    return SparkSession.builder \
        .appName("medallion-bronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def hdfs_has_data(spark, path, timeout=600, interval=20):
    """Tunggu sampai path HDFS ada dan punya minimal 1 file JSON."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(HDFS_BASE)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)

    elapsed = 0
    while elapsed < timeout:
        if fs.exists(hdfs_path) and len(list(fs.listStatus(hdfs_path))) > 0:
            return True
        print(f"Menunggu data di {path} ({elapsed}s)...")
        time.sleep(interval)
        elapsed += interval

    raise TimeoutError(f"Data tidak ditemukan di {path} setelah {timeout}s")


class BronzeIngestion:
    def __init__(self, spark):
        self.spark = spark

    def ingest_weather_api(self):
        df = self.spark.read.option("multiLine", True).json(HDFS_API_PATH)
        result = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("weather-api")) \
            .dropDuplicates(["kode_kota", "timestamp"])
        result.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_api")
        print(f"weather_api: {result.count()} records")

    def ingest_weather_rss(self):
        df = self.spark.read.option("multiLine", True).json(HDFS_RSS_PATH)
        result = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("weather-rss")) \
            .dropDuplicates(["judul", "sumber"])
        result.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/weather_rss")
        print(f"weather_rss : {result.count()} records")


def main():
    spark = build_spark()

    print("="*70)
    print("BRONZE LAYER: Raw Data Ingestion from HDFS")
    print("="*70)

    hdfs_has_data(spark, HDFS_API_PATH)
    hdfs_has_data(spark, HDFS_RSS_PATH)

    ingestion = BronzeIngestion(spark)

    print("\nIngesting weather API...")
    ingestion.ingest_weather_api()

    print("Ingesting weather RSS...")
    ingestion.ingest_weather_rss()

    print("\nBronze layer complete")
    print("="*70)

    spark.stop()


if __name__ == "__main__":
    main()
