import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    hour, avg, max, min, col, count, round as spark_round
)


LAKEHOUSE_PATH = "/lakehouse"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver"
# GoldPath = f"{LAKEHOUSE_PATH}/gold"


def build_spark():
    return SparkSession.builder \
        .appName("WeatherPulseAnalysis") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


class WeatherAnalysis:
    def __init__(self, spark):
        self.spark = spark
        self.df = spark.read.format("delta").load(f"{SILVER_PATH}/weather_api")
        self.df.createOrReplaceTempView("weather_api")

    def suhu_per_kota(self):
        return (
            self.df.groupBy("kode_kota", "nama_kota")
            .agg(
                spark_round(avg("temperature"), 2).alias("suhu_avg"),
                max("temperature").alias("suhu_tertinggi"),
                min("temperature").alias("suhu_terendah"),
                count("*").alias("jumlah_event"),
            )
            .orderBy(col("suhu_avg").desc())
        )

    def kondisi_ekstrem(self):
        return (
            self.df.filter(
                (col("wind_speed") > 40) | (col("humidity") > 90) | (col("temperature") > 35)
            )
            .groupBy("kode_kota", "nama_kota")
            .agg(count("*").alias("jumlah_event_ekstrem"))
            .orderBy(col("jumlah_event_ekstrem").desc())
        )

    def tren_jam(self):
        return self.spark.sql("""
            SELECT
                HOUR(timestamp) AS jam,
                ROUND(AVG(temperature), 2) AS suhu_avg,
                COUNT(*) AS jumlah_event
            FROM weather_api
            WHERE timestamp IS NOT NULL
            GROUP BY HOUR(timestamp)
            ORDER BY jam
        """)


class NewsAnalysis:
    def __init__(self, spark):
        self.spark = spark
        self.df = spark.read.format("delta").load(f"{SILVER_PATH}/weather_rss")


def build_narasi(suhu_df, ekstrem_df, tren_df):
    top_suhu = suhu_df.first()
    narasi_suhu = (
        f"{top_suhu['nama_kota']} memiliki rata-rata suhu tertinggi "
        f"({top_suhu['suhu_avg']} C). Kota ini perlu diprioritaskan untuk "
        "monitoring pengiriman pada jam panas."
    ) if top_suhu else "Data suhu belum tersedia."

    top_ekstrem = ekstrem_df.first()
    narasi_ekstrem = (
        f"{top_ekstrem['nama_kota']} paling sering mengalami kondisi ekstrem "
        f"({top_ekstrem['jumlah_event_ekstrem']} event). Rute melalui kota ini "
        "perlu dicek ulang sebelum pengiriman."
    ) if top_ekstrem else (
        "Tidak ada event ekstrem (wind_speed > 40, humidity > 90, temperature > 35). "
        "Kondisi relatif aman."
    )

    jam_terdingin = tren_df.orderBy(col("suhu_avg").asc()).first()
    narasi_tren = (
        f"Jam {jam_terdingin['jam']:02d}:00 memiliki rata-rata suhu terendah "
        f"({jam_terdingin['suhu_avg']} C). Waktu ini dapat dipertimbangkan "
        "sebagai waktu pengiriman yang lebih nyaman."
    ) if jam_terdingin else "Data timestamp belum cukup untuk membaca tren suhu per jam."

    return narasi_suhu, narasi_ekstrem, narasi_tren


def main():
    spark = build_spark()

    print("="*70)
    print("SPARK ANALYSIS - baca dari Silver Delta")
    print("="*70)

    weather = WeatherAnalysis(spark)
    news = NewsAnalysis(spark)

    total_event = weather.df.count()
    print(f"Total event cuaca dari Silver: {total_event}")

    suhu_df = weather.suhu_per_kota()
    ekstrem_df = weather.kondisi_ekstrem()
    tren_df = weather.tren_jam()

    total_ekstrem = weather.df.filter(
        (col("wind_speed") > 40) | (col("humidity") > 90) | (col("temperature") > 35)
    ).count()

    print("\nAnalisis 1 - Statistik Suhu Per Kota")
    suhu_df.show(truncate=False)

    print("\nAnalisis 2 - Deteksi Kondisi Ekstrem")
    ekstrem_df.show(truncate=False)
    print(f"Total event ekstrem: {total_ekstrem}")

    print("\nAnalisis 3 - Tren Suhu Per Jam")
    tren_df.show(24, truncate=False)

    narasi_suhu, narasi_ekstrem, narasi_tren = build_narasi(suhu_df, ekstrem_df, tren_df)
    print(f"\nInterpretasi suhu  : {narasi_suhu}")
    print(f"Interpretasi ekstrem: {narasi_ekstrem}")
    print(f"Interpretasi tren  : {narasi_tren}")

    os.makedirs("dashboard/data", exist_ok=True)

    spark_results = {
        "metadata": {
            "topik": "WeatherPulse",
            "silver_input": f"{SILVER_PATH}/weather_api",
            "total_event": total_event,
            "total_event_ekstrem": total_ekstrem,
        },
        "narasi": {
            "suhu_per_kota": narasi_suhu,
            "kondisi_ekstrem": narasi_ekstrem,
            "tren_jam": narasi_tren,
        },
        "suhu_per_kota": suhu_df.toPandas().to_dict(orient="records"),
        "kondisi_ekstrem": ekstrem_df.toPandas().to_dict(orient="records"),
        "tren_jam": tren_df.toPandas().to_dict(orient="records"),
    }

    with open("dashboard/data/spark_results.json", "w", encoding="utf-8") as f:
        json.dump(spark_results, f, indent=2, ensure_ascii=False)

    print("\ndashboard/data/spark_results.json berhasil dibuat")
    print("="*70)

    spark.stop()


if __name__ == "__main__":
    main()
