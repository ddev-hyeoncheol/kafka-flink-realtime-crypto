FROM flink:1.18.1-java11

# Install wget tool to fetch remote dependencies safely
USER root
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Switch directory to Flink's runtime library path
WORKDIR /opt/flink/lib

# 1. Download official Apache Flink Kafka SQL Connector
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# 2. Download Flink Redis Connector (Shaded jar-with-dependencies version including Lettuce/Netty dependencies)
RUN wget https://repo1.maven.org/maven2/io/github/jeff-zou/flink-connector-redis/1.4.3/flink-connector-redis-1.4.3-jar-with-dependencies.jar

# Change ownership of all downloaded libraries to avoid permission issues
RUN chown -R flink:flink /opt/flink/lib/

# Revert back to the default unprivileged flink user for container execution
USER flink
