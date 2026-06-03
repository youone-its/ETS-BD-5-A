"""
Orchestration Script - Jalankan seluruh pipeline WeatherPulse.

Pipeline stages:
1. Producer API (fetch data cuaca)
2. Producer RSS (fetch berita cuaca)
3. Consumer to HDFS (simpan Kafka ke HDFS)
4. Medallion Layers (Bronze → Silver → Gold)
5. Spark Analysis (analisis data)
"""

import subprocess
import time
import threading
import sys
from datetime import datetime


def run_command(cmd: list, name: str, daemon: bool = False) -> threading.Thread:
    """Jalankan command dalam thread terpisah."""
    def worker():
        try:
            print(f"\n[{name}] Dimulai pada {datetime.now().strftime('%H:%M:%S')}")
            result = subprocess.run(cmd, check=False)
            if result.returncode != 0:
                print(f"[{name}] Selesai dengan error code: {result.returncode}")
        except Exception as e:
            print(f"[{name}] Error: {e}")

    thread = threading.Thread(target=worker, daemon=daemon, name=name)
    thread.start()
    return thread


def main():
    print("="*70)
    print("🚀 WEATHERPULSE PIPELINE ORCHESTRATION")
    print("="*70)
    print("\nMemulai seluruh pipeline: Producer → Consumer → Medallion → Analysis")

    threads = {}

    try:
        # Stage 1: Producers (berjalan background)
        print("\n[STAGE 1] Menjalankan Producers...")
        threads["producer_api"] = run_command(
            ["python", "producer_api.py"],
            "Producer API",
            daemon=True
        )
        time.sleep(2)

        threads["producer_rss"] = run_command(
            ["python", "producer_rss.py"],
            "Producer RSS",
            daemon=True
        )
        time.sleep(2)

        # Stage 2: Consumer
        print("\n[STAGE 2] Menjalankan Consumer to HDFS...")
        threads["consumer"] = run_command(
            ["python", "consumer_to_hdfs.py"],
            "Consumer HDFS",
            daemon=True
        )
        time.sleep(5)

        # Stage 3: Medallion Layers
        print("\n[STAGE 3] Menjalankan Medallion Architecture...")
        print("\n  → Bronze layer...")
        run_command(["python", "medallion/01_bronze.py"], "Bronze Layer").join()
        time.sleep(2)

        print("\n  → Silver layer...")
        run_command(["python", "medallion/02_silver.py"], "Silver Layer").join()
        time.sleep(2)

        print("\n  → Gold layer...")
        run_command(["python", "medallion/03_gold.py"], "Gold Layer").join()
        time.sleep(2)

        # Stage 4: Analysis
        print("\n[STAGE 4] Menjalankan Spark Analysis...")
        run_command(["python", "spark_analysis.py"], "Spark Analysis").join()

        print("\n" + "="*70)
        print("✅ PIPELINE SELESAI!")
        print("="*70)
        print("\n📊 Hasil analisis tersimpan di: dashboard/data/spark_results.json")
        print("🔄 Producers masih berjalan di background. Tekan Ctrl+C untuk hentikan.")
        print("="*70 + "\n")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\n⏹️  Menghentikan pipeline...")
        print("Silakan tunggu threads selesai...")
        time.sleep(5)
        sys.exit(0)


if __name__ == "__main__":
    main()
