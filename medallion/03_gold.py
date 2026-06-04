import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, avg, min, max, count,
    rank, lag, window
)
from pyspark.sql.window import Window as WindowSpec

LAKEHOUSE_PATH = "/lakehouse"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver"
GOLD_PATH = f"{LAKEHOUSE_PATH}/gold"


def build_spark():
    return SparkSession.builder \
        .appName("medallion-gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


class WeatherGold:
    def __init__(self, spark):
        self.spark = spark
        self.df = spark.read.format("delta").load(f"{SILVER_PATH}/weather_api")

    def analytics(self):
        # order by unix epoch (long) so rangeBetween dengan detik bisa dipakai
        window_spec = WindowSpec.partitionBy("kode_kota").orderBy(col("timestamp").cast("long"))
        return self.df \
            .withColumn("temp_lag1", lag("temperature").over(window_spec)) \
            .withColumn("temp_change", col("temperature") - col("temp_lag1")) \
            .withColumn("temp_ma3", avg("temperature").over(
                window_spec.rangeBetween(-2 * 3600, 0)
            )) \
            .select(
                col("kode_kota"),
                col("nama_kota"),
                col("temperature").alias("temp_current"),
                col("temp_change").alias("temp_change_1h"),
                col("temp_ma3").alias("temp_moving_avg_3h"),
                col("humidity"),
                col("wind_speed"),
                col("timestamp").alias("reading_time"),
                current_timestamp().alias("gold_created_at"),
            )

    def extremes(self):
        return self.df \
            .groupBy("kode_kota", "nama_kota") \
            .agg(
                max("temperature").alias("max_temp"),
                min("temperature").alias("min_temp"),
                avg("temperature").alias("avg_temp"),
                max("humidity").alias("max_humidity"),
                max("wind_speed").alias("max_wind"),
                count("*").alias("reading_count"),
            ) \
            .withColumn("temp_range", col("max_temp") - col("min_temp"))

    def weather_hour(self):
        return self.df \
            .withColumn("hour", window(col("timestamp"), "1 hour")) \
            .groupBy("kode_kota", "nama_kota", "hour") \
            .agg(
                avg("temperature").alias("avg_temp"),
                max("humidity").alias("max_humidity"),
                max("wind_speed").alias("max_wind"),
            )


class NewsGold:
    def __init__(self, spark):
        self.spark = spark
        self.df = spark.read.format("delta").load(f"{SILVER_PATH}/weather_rss")

    def by_source(self):
        return self.df \
            .groupBy("sumber") \
            .agg(
                count("*").alias("article_count"),
                max("waktu_terbit").alias("latest_article"),
            ) \
            .orderBy(col("article_count").desc())

    def recent(self, n=20):
        window_spec = WindowSpec.orderBy(col("waktu_terbit").desc())
        return self.df \
            .withColumn("news_rank", rank().over(window_spec)) \
            .filter(col("news_rank") <= n) \
            .select(
                col("news_rank"),
                col("judul"),
                col("sumber"),
                col("waktu_terbit"),
                col("ringkasan"),
                current_timestamp().alias("gold_created_at"),
            )

    def news_hour(self):
        return self.df \
            .withColumn("hour", window(col("waktu_terbit"), "1 hour")) \
            .groupBy("hour") \
            .agg(count("*").alias("news_count"))


def save(df, path):
    df.write.format("delta").mode("overwrite").save(path)


def main():
    spark = build_spark()

    print("="*70)
    print("GOLD LAYER: Business Analytics")
    print("="*70)

    weather = WeatherGold(spark)
    news = NewsGold(spark)

    print("\nWeather analytics...")
    wa = weather.analytics()
    we = weather.extremes()
    save(wa, f"{GOLD_PATH}/weather_analytics")
    save(we, f"{GOLD_PATH}/weather_extremes")
    print(f"weather_analytics: {wa.count()} records")
    print(f"weather_extremes : {we.count()} kota")

    print("\nNews analytics...")
    ns = news.by_source()
    rn = news.recent()
    save(ns, f"{GOLD_PATH}/news_by_source")
    save(rn, f"{GOLD_PATH}/recent_news")
    print(f"news_by_source: {ns.count()} sumber")
    print(f"recent_news   : {rn.count()} artikel")

    print("\nWeather-news correlation...")
    wh = weather.weather_hour()
    nh = news.news_hour()
    correlation = wh.join(nh, on="hour", how="left").fillna(0, subset=["news_count"])
    save(correlation, f"{GOLD_PATH}/weather_news_correlation")
    print(f"correlation: {correlation.count()} records")

    print("\n" + "="*70)
    print("Gold layer complete")
    print("="*70)

    spark.stop()


if __name__ == "__main__":
    main()
