docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --topic weather-api \
  --from-beginning \
  --max-messages 5 \
  --bootstrap-server localhost:9092

docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --topic weather-rss \
  --from-beginning \
  --max-messages 5 \
  --bootstrap-server localhost:9092
