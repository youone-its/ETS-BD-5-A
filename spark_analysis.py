# """
# Spark Analysis - Analisis data cuaca dari HDFS.
# Melakukan 3 analisis:
# 1. Statistik suhu per kota
# 2. Deteksi kondisi ekstrem
# 3. Tren suhu per jam
# """

# import os
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     hour, avg, max, min, col, count, to_timestamp, round as spark_round
# )

# from config import HDFS_URI, HDFS_API_PATH


# def main():
#     os.environ["HADOOP_USER_NAME"] = "hadoop"

#     spark = SparkSession.builder \
#         .appName("WeatherPulseAnalysis") \
#         .config("spark.hadoop.fs.defaultFS", HDFS_URI) \
#         .getOrCreate()

#     print("="*70)
#     print("📊 ANALISIS DATA CUACA - SPARK")
#     print("="*70)

#     df_api = spark.read.json(f"{HDFS_URI}{HDFS_API_PATH}/")
#     df_api = df_api.withColumn("ts_obj", to_timestamp(col("timestamp")))
#     df_api.createOrReplaceTempView("weather_api")

#     total_event = df_api.count()
#     print(f"\nTotal event cuaca terbaca dari HDFS: {total_event}")

#     # ========== ANALISIS 1: Statistik Suhu Per Kota ==========
#     print("\n" + "="*70)
#     print("ANALISIS 1 - Statistik Suhu Per Kota")
#     print("="*70)

#     suhu_per_kota = (
#         df_api.groupBy("kode_kota", "nama_kota")
#         .agg(
#             spark_round(avg("temperature"), 2).alias("suhu_avg"),
#             max("temperature").alias("suhu_tertinggi"),
#             min("temperature").alias("suhu_terendah"),
#             count("*").alias("jumlah_event"),
#         )
#         .orderBy(col("suhu_avg").desc())
#     )

#     suhu_per_kota.show(truncate=False)

#     top_suhu = suhu_per_kota.first()
#     if top_suhu:
#         narasi_suhu = (
#             f"{top_suhu['nama_kota']} memiliki rata-rata suhu tertinggi "
#             f"({top_suhu['suhu_avg']} °C). Kota ini perlu diprioritaskan untuk "
#             "monitoring pengiriman pada jam panas."
#         )
#     else:
#         narasi_suhu = "Data suhu belum tersedia untuk dianalisis."

#     print(f"\n📝 Interpretasi: {narasi_suhu}")

#     # ========== ANALISIS 2: Deteksi Kondisi Ekstrem ==========
#     print("\n" + "="*70)
#     print("ANALISIS 2 - Deteksi Kondisi Cuaca Ekstrem")
#     print("="*70)

#     kondisi_ekstrem = df_api.filter(
#         (col("wind_speed") > 40) | (col("humidity") > 90) | (col("temperature") > 35)
#     )

#     rekap_ekstrem = (
#         kondisi_ekstrem.groupBy("kode_kota", "nama_kota")
#         .agg(count("*").alias("jumlah_event_ekstrem"))
#         .orderBy(col("jumlah_event_ekstrem").desc())
#     )

#     total_ekstrem = kondisi_ekstrem.count()
#     rekap_ekstrem.show(truncate=False)
#     print(f"\nTotal event cuaca ekstrem: {total_ekstrem}")

#     top_ekstrem = rekap_ekstrem.first()
#     if top_ekstrem:
#         narasi_ekstrem = (
#             f"{top_ekstrem['nama_kota']} paling sering mengalami kondisi ekstrem "
#             f"({top_ekstrem['jumlah_event_ekstrem']} event). Rute melalui kota ini "
#             "perlu dicek ulang sebelum pengiriman."
#         )
#     else:
#         narasi_ekstrem = (
#             "Tidak ada event ekstrem berdasarkan ambang wind_speed > 40 km/h, "
#             "humidity > 90%, atau temperature > 35°C. Kondisi relatif aman pada "
#             "data yang terkumpul."
#         )

#     print(f"\n📝 Interpretasi: {narasi_ekstrem}")

#     # ========== ANALISIS 3: Tren Suhu Per Jam ==========
#     print("\n" + "="*70)
#     print("ANALISIS 3 - Tren Suhu Rata-Rata Per Jam")
#     print("="*70)

#     tren_jam = spark.sql(
#         """
#         SELECT
#             HOUR(ts_obj) AS jam,
#             ROUND(AVG(temperature), 2) AS suhu_avg,
#             COUNT(*) AS jumlah_event
#         FROM weather_api
#         WHERE ts_obj IS NOT NULL
#         GROUP BY HOUR(ts_obj)
#         ORDER BY jam
#         """
#     )

#     tren_jam.show(24, truncate=False)

#     jam_terdingin = tren_jam.orderBy(col("suhu_avg").asc()).first()
#     if jam_terdingin:
#         narasi_tren = (
#             f"Jam {jam_terdingin['jam']:02d}:00 memiliki rata-rata suhu terendah "
#             f"({jam_terdingin['suhu_avg']}°C). Waktu ini dapat dipertimbangkan "
#             "sebagai waktu pengiriman yang lebih nyaman."
#         )
#     else:
#         narasi_tren = "Data timestamp belum cukup untuk membaca tren suhu per jam."

#     print(f"\n📝 Interpretasi: {narasi_tren}")

#     # ========== SIMPAN HASIL ANALISIS ==========
#     print("\n" + "="*70)
#     print("💾 MENYIMPAN HASIL ANALISIS")
#     print("="*70)

#     hdfs_output = f"{HDFS_URI}/data/weather/hasil"

#     suhu_per_kota.write.mode("overwrite").json(f"{hdfs_output}/suhu_per_kota")
#     rekap_ekstrem.write.mode("overwrite").json(f"{hdfs_output}/kondisi_ekstrem")
#     tren_jam.write.mode("overwrite").json(f"{hdfs_output}/tren_jam")

#     print(f"✅ Hasil analisis tersimpan di {hdfs_output}/")

#     os.makedirs("dashboard/data", exist_ok=True)

#     spark_results = {
#         "metadata": {
#             "topik": "WeatherPulse",
#             "hdfs_input": f"{HDFS_URI}{HDFS_API_PATH}/",
#             "hdfs_output": hdfs_output,
#             "total_event": total_event,
#             "total_event_ekstrem": total_ekstrem,
#         },
#         "narasi": {
#             "suhu_per_kota": narasi_suhu,
#             "kondisi_ekstrem": narasi_ekstrem,
#             "tren_jam": narasi_tren,
#         },
#         "suhu_per_kota": suhu_per_kota.toPandas().to_dict(orient="records"),
#         "kondisi_ekstrem": rekap_ekstrem.toPandas().to_dict(orient="records"),
#         "tren_jam": tren_jam.toPandas().to_dict(orient="records"),
#     }

#     with open("dashboard/data/spark_results.json", "w", encoding="utf-8") as f:
#         json.dump(spark_results, f, indent=2, ensure_ascii=False)

#     print("✅ dashboard/data/spark_results.json berhasil dibuat")
#     print("="*70 + "\n")

#     spark.stop()


# if __name__ == "__main__":
#     main()
