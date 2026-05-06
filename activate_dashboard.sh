#!/bin/bash
# WeatherPulse Dashboard - Activation Script
# Activates virtual environment and starts Flask server

echo "🚀 WeatherPulse Dashboard - Startup Script"
echo "==========================================="
echo ""

# Activate virtual environment
echo "📦 Activating virtual environment..."
source dashboard_env/bin/activate

echo "✅ Virtual environment activated!"
echo ""
echo "🎯 Next steps:"
echo "  1. Make sure Kafka & Hadoop are running:"
echo "     - cd hadoop && docker-compose ps"
echo "     - cd kafka && docker-compose ps"
echo ""
echo "  2. In separate terminals, start:"
echo "     - kafka/producer_api.py"
echo "     - kafka/producer_rss.py"
echo "     - kafka/consumer_to_hdfs.py"
echo "     - spark/analysis.py"
echo ""
echo "  3. Then start dashboard:"
echo "     - cd dashboard"
echo "     - python app.py"
echo ""
echo "🌐 Dashboard will be available at: http://localhost:5000"
echo ""
