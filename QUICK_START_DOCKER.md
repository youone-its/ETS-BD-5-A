# ⚡ Quick Start - Docker Compose

**TL;DR**: Run full WeatherPulse stack with one command!

## 🚀 Start Everything

```bash
cd /path/to/ETS-BD-5-A
docker-compose up -d
```

## ⏳ Wait for Services

Monitor startup (takes ~5-10 minutes):
```bash
docker-compose logs -f
```

Watch for these messages:
- ✅ `kafka-broker: started`
- ✅ `Namenode started`
- ✅ `Gold layer complete!`
- ✅ `Server starting at http://localhost:5000`

## 🌐 Access Dashboard

```
http://localhost:5000
```

## 📊 What's Running

| Service | Port | Status |
|---------|------|--------|
| **Dashboard** ⭐ | **5000** | **http://localhost:5000** |
| Kafka | 9092 | Internal |
| HDFS NameNode | 9870 | http://localhost:9870 |
| Yarn | 8088 | http://localhost:8088 |

## ✅ Verify Integration

```bash
# Check health
curl http://localhost:5000/api/health | jq

# Get real-time data
curl http://localhost:5000/api/current_weather | jq '.JKT'

# Get Gold layer analytics
curl http://localhost:5000/api/gold/weather_analytics | jq '.data[0]'
curl http://localhost:5000/api/gold/weather_news_correlation | jq '.data[0]'
```

## 🛑 Stop Everything

```bash
docker-compose down
```

## 📚 Need Help?

- See **DOCKER_COMPOSE_GUIDE.md** for detailed documentation
- See **MEDALLION_INTEGRATION.md** for architecture details
- See **ENDPOINTS_REFERENCE.md** for API documentation

---

**That's it!** Dashboard is now available at `http://localhost:5000` 🎉
