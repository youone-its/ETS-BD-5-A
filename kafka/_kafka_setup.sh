#!/bin/bash

#bikin topic
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic weather-api --partitions 6 --replication-factor 1 --config retention.ms=86400000 --bootstrap-server localhost:9092
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic weather-rss --partitions 6 --replication-factor 1 --config retention.ms=86400000 --bootstrap-server localhost:9092

#cek topic yang dibikin
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
