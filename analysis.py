import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, max, min, col, count, to_timestamp

spark = SparkSession.builder \
    .appName("WeatherPulseAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

print("Reading data from HDFS...")
df_api = spark.read.option("multiLine", True).json("/data/weather/api/*.json")

df_api = df_api.withColumn("ts_obj", to_timestamp(col("timestamp")))

# --- ANALISIS 1: Perbandingan Suhu Antar Kota ---

print("\n--- ANALISIS 1: Statistik Suhu per Kota ---")
suhu_per_kota = df_api.groupBy("nama_kota").agg(
    avg("temperature").alias("suhu_avg"),
    max("temperature").alias("suhu_tertinggi"),
    min("temperature").alias("suhu_terendah")
).orderBy("suhu_avg", ascending=False)
suhu_per_kota.show()

# --- ANALISIS 2: Deteksi Kondisi Ekstrem ---

print("\n--- ANALISIS 2: Kejadian Cuaca Ekstrem ---")
kondisi_ekstrem = df_api.filter(
    (col("wind_speed") > 40) | (col("humidity") > 90) | (col("temperature") > 35)
)
rekap_ekstrem = kondisi_ekstrem.groupBy("nama_kota").agg(
    count("*").alias("jumlah_event_ekstrem")
).orderBy("jumlah_event_ekstrem", ascending=False)
rekap_ekstrem.show()

# --- ANALISIS 3: Tren Suhu Per Jam ---
# Melihat pola diurnal suhu rata-rata semua kota[cite: 1]
print("\n--- ANALISIS 3: Tren Suhu per Jam ---")
tren_jam = df_api.withColumn("jam", hour(col("ts_obj"))) \
    .groupBy("jam") \
    .agg(avg("temperature").alias("suhu_avg")) \
    .orderBy("jam")
tren_jam.show(24)

# --- PENYIMPANAN HASIL (Untuk Dashboard) ---
# Menyimpan ringkasan sebagai JSON lokal agar dibaca Flask[cite: 1]
print("\nSaving results for dashboard...")
results_path = "../dashboard/data/spark_results.json"
os.makedirs(os.path.dirname(results_path), exist_ok=True)

# Konversi hasil ke Pandas untuk disimpan sebagai JSON lokal
# Pastikan folder dashboard/data ada[cite: 1]
final_results = {
    "suhu_kota": suhu_per_kota.toPandas().to_dict(orient="records"),
    "ekstrem": rekap_ekstrem.toPandas().to_dict(orient="records"),
    "tren_jam": tren_jam.toPandas().to_dict(orient="records")
}

with open(results_path, "w") as f:
    import json
    json.dump(final_results, f, indent=4)

print(f"✅ Analisis selesai. Hasil disimpan di {results_path}")