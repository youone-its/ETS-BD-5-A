FROM apache/kafka:3.9.0
ENV KAFKA_LOG_DIRS=/var/lib/kafka/data
USER root
RUN mkdir -p /var/lib/kafka/data && chown -R 1000:1000 /var/lib/kafka
USER 1000
CMD ["/opt/kafka/bin/kafka-server-start.sh", "/opt/kafka/config/server.properties"]
