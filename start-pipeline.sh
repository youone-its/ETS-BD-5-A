#!/bin/bash

# Quick start script untuk WeatherPulse Pipeline di Docker
# Usage: ./start-pipeline.sh

set -e

echo "=================================="
echo "🚀 WeatherPulse Pipeline - Docker"
echo "=================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check docker
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ docker-compose not found${NC}"
    exit 1
fi

echo -e "\n${YELLOW}📋 Step 1: Building images...${NC}"
docker-compose build --no-cache 2>&1 | grep -E "Building|Successfully" | tail -5

echo -e "\n${YELLOW}📋 Step 2: Starting infrastructure (Kafka, Hadoop)...${NC}"
docker-compose up -d kafka namenode datanode resourcemanager nodemanager
sleep 15
echo -e "${GREEN}✅ Infrastructure ready${NC}"

echo -e "\n${YELLOW}📋 Step 3: Starting data pipeline...${NC}"
docker-compose up -d producer-api producer-rss consumer-hdfs
sleep 10
echo -e "${GREEN}✅ Producers and Consumer started${NC}"

echo -e "\n${YELLOW}📋 Step 4: Processing data (wait 30s for accumulation)...${NC}"
sleep 30

echo -e "\n${YELLOW}📋 Step 5: Running Medallion layers...${NC}"
docker-compose run --rm medallion-bronze
docker-compose run --rm medallion-silver
docker-compose run --rm medallion-gold
echo -e "${GREEN}✅ Medallion layers complete${NC}"

echo -e "\n${YELLOW}📋 Step 6: Running Spark analysis...${NC}"
docker-compose run --rm spark-analysis
echo -e "${GREEN}✅ Analysis complete${NC}"

echo -e "\n${YELLOW}📋 Step 7: Starting Dashboard...${NC}"
docker-compose up -d dashboard
echo -e "${GREEN}✅ Dashboard ready at http://localhost:5000${NC}"

echo -e "\n${YELLOW}📋 Checking logs...${NC}"
echo ""
docker-compose logs --tail=20

echo -e "\n=================================="
echo -e "${GREEN}✅ PIPELINE COMPLETE!${NC}"
echo -e "==================================\n"

echo "📊 Next steps:"
echo "  1. Open browser: http://localhost:5000"
echo "  2. Check results in dashboard"
echo "  3. View logs: docker-compose logs -f"
echo "  4. Stop: docker-compose down"
echo ""
