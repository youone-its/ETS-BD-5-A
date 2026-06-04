FROM apache/spark:3.5.0-python3
USER root

# Install Delta Lake
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar

# Copy and install Python requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app
COPY . /app

# Set environment variables for Delta support
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYSPARK_PYTHON=python3
ENV SPARK_SQL_EXTENSIONS=io.delta.sql.DeltaSparkSessionExtension
ENV SPARK_CATALOG=org.apache.spark.sql.delta.catalog.DeltaCatalog

CMD ["/bin/bash"]
