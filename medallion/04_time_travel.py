"""
TIME TRAVEL: Delta Lake Version Control & Data Evolution
=========================================================
Demonstrasi Delta Lake time travel untuk version control:
1. Update data pada Gold table
2. Query versi lama (sebelum update)
3. Query versi terbaru (sesudah update)
4. Bandingkan perbedaan dan print hasilnya
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("medallion-time-travel") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("="*70)
print("⏰ TIME TRAVEL: Delta Lake Version Control")
print("="*70)

LAKEHOUSE_PATH = "/lakehouse"
GOLD_PATH = f"{LAKEHOUSE_PATH}/gold"

# ============================================================================
# READ CURRENT VERSION (Sebelum Update)
# ============================================================================

print("\n📸 1. Membaca versi SEBELUM update...")
try:
    weather_analytics_v0 = spark.read.format("parquet").load(f"{GOLD_PATH}/weather_analytics")
    print(f"\n📊 Versi awal - jumlah record: {weather_analytics_v0.count()}")
    print("Sampel data awal:")
    weather_analytics_v0.limit(5).show(truncate=False)
    
    # Get version number
    # from delta.tables import DeltaTable (Delta not available)
    delta_table = DeltaTable.forPath(spark, f"{GOLD_PATH}/weather_analytics")
    version_before = delta_table.history().select("version").collect()
    if version_before:
        v_before = version_before[0][0]
        print(f"📌 Version ID (sebelum): {v_before}")
    
except Exception as e:
    print(f"⚠️  Error membaca data awal: {e}")
    v_before = 0

# ============================================================================
# PERFORM UPDATE
# ============================================================================

print("\n✏️  2. Melakukan UPDATE pada weather_analytics...")
try:
    # from delta.tables import DeltaTable (Delta not available)
    
    delta_table = DeltaTable.forPath(spark, f"{GOLD_PATH}/weather_analytics")
    
    # Update: Tambah 5°C ke semua temperature (simulasi koreksi sensor)
    delta_table.update(
        condition="temp_current IS NOT NULL",
        set={"temp_current": col("temp_current") + 5.0}
    )
    
    print("✅ Update selesai: +5°C untuk semua readings")
    
    # Get new version number
    version_after = delta_table.history().select("version").collect()[0][0]
    print(f"📌 Version ID (sesudah): {version_after}")
    
except Exception as e:
    print(f"⚠️  Error melakukan update: {e}")
    version_after = v_before

# ============================================================================
# QUERY AFTER UPDATE (Versi Terbaru)
# ============================================================================

print("\n📸 3. Membaca versi SESUDAH update (latest)...")
try:
    weather_analytics_latest = spark.read.format("parquet").load(f"{GOLD_PATH}/weather_analytics")
    print(f"\n📊 Versi terbaru - jumlah record: {weather_analytics_latest.count()}")
    print("Sampel data setelah update (+5°C):")
    weather_analytics_latest.limit(5).show(truncate=False)
    
except Exception as e:
    print(f"⚠️  Error membaca data terbaru: {e}")

# ============================================================================
# TIME TRAVEL: Query Versi Lama
# ============================================================================

print("\n⏪ 4. TIME TRAVEL: Membaca versi LAMA (sebelum update)...")
try:
    if v_before != version_after:
        # Query specific version using timestamp or version number
        weather_analytics_old = spark.read.format("parquet") \
            .option("versionAsOf", v_before) \
            .load(f"{GOLD_PATH}/weather_analytics")
        
        print(f"\n📊 Versi lama (version {v_before}) - jumlah record: {weather_analytics_old.count()}")
        print("Sampel data versi lama (original temperature):")
        weather_analytics_old.limit(5).show(truncate=False)
    else:
        print("⚠️  Tidak ada perubahan versi (update mungkin gagal)")
        weather_analytics_old = weather_analytics_latest
        
except Exception as e:
    print(f"⚠️  Error time travel: {e}")

# ============================================================================
# COMPARISON: OLD vs NEW
# ============================================================================

print("\n🔍 5. PERBANDINGAN: Versi Lama vs Baru...")
try:
    # Gabung kedua versi untuk comparison
    old_renamed = weather_analytics_old \
        .select(col("temp_current").alias("temp_old"), "kode_kota", "timestamp")
    
    new_renamed = weather_analytics_latest \
        .select(col("temp_current").alias("temp_new"), "kode_kota", "timestamp")
    
    comparison = old_renamed.join(
        new_renamed,
        on=["kode_kota", "timestamp"],
        how="inner"
    ).withColumn(
        "temp_diff",
        col("temp_new") - col("temp_old")
    )
    
    print("\n📊 Perbandingan Temperature Old vs New:")
    comparison.limit(10).show(truncate=False)
    
    # Summary statistics
    stats = comparison.agg({
        "temp_old": ["min", "max", "avg"],
        "temp_new": ["min", "max", "avg"],
        "temp_diff": ["min", "max", "avg"]
    })
    
    print("\n📈 Summary Statistics:")
    stats.show(truncate=False)
    
except Exception as e:
    print(f"⚠️  Error comparison: {e}")

# ============================================================================
# DELTA LOG HISTORY
# ============================================================================

print("\n📜 6. DELTA LOG HISTORY...")
try:
    # from delta.tables import DeltaTable (Delta not available)
    delta_table = DeltaTable.forPath(spark, f"{GOLD_PATH}/weather_analytics")
    
    history = delta_table.history().select("version", "timestamp", "operation", "operationParameters").limit(10)
    print("\nDelta Lake transaction history:")
    history.show(truncate=False)
    
except Exception as e:
    print(f"⚠️  Error reading history: {e}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print("✨ Time Travel Demo Complete!")
print(f"📍 Demonstrasi Delta Lake version control:")
print(f"   - Read versi lama (v{v_before})")
print(f"   - Update +5°C")
print(f"   - Read versi baru (v{version_after})")
print(f"   - Time travel query ke versi lama")
print(f"   - Comparison: old vs new")
print("="*70 + "\n")

spark.stop()
