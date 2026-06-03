#!/bin/bash

# Docker Entrypoint untuk orchestration pipeline
# Usage: docker-entrypoint.sh [producer-api|producer-rss|consumer|medallion|analysis]

set -e

SERVICE=$1

echo "================================"
echo "WeatherPulse Pipeline - $SERVICE"
echo "================================"

case $SERVICE in
  producer-api)
    echo "🌡️  Starting Producer API..."
    exec python producer_api.py
    ;;

  producer-rss)
    echo "📰 Starting Producer RSS..."
    exec python producer_rss.py
    ;;

  consumer)
    echo "💾 Starting Consumer to HDFS..."
    mkdir -p /data/weather/api
    mkdir -p /data/weather/rss
    exec python consumer_to_hdfs.py
    ;;

  medallion)
    echo "🏛️  Starting Medallion Architecture..."
    cd /app/medallion
    echo "  → Bronze layer..."
    python 01_bronze.py || true
    sleep 3

    echo "  → Silver layer..."
    python 02_silver.py || true
    sleep 3

    echo "  → Gold layer..."
    python 03_gold.py || true
    sleep 3

    echo "✅ Medallion layers complete!"
    ;;

  analysis)
    echo "📊 Starting Spark Analysis..."
    cd /app
    exec python spark_analysis.py
    ;;

  *)
    echo "Unknown service: $SERVICE"
    echo "Available: producer-api, producer-rss, consumer, medallion, analysis"
    exit 1
    ;;
esac
