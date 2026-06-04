FROM apache/spark:3.5.0-python3
USER root

# Install wget
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Copy and install Python requirements (includes delta-spark)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Download Delta Lake JARs — delta-core 3.x was renamed to delta-spark, delta-storage is a required dep
RUN mkdir -p /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    -O /opt/spark/jars/delta-spark_2.12-3.1.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
    -O /opt/spark/jars/delta-storage-3.1.0.jar

WORKDIR /app
COPY . /app

# Spark + Delta configuration
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["/bin/bash"]
