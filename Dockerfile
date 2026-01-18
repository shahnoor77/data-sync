FROM python:3.11-slim

# Set versions
ENV DEBEZIUM_VERSION=2.5.0.Final
ENV KAFKA_CONNECT_VERSION=3.6.0
ENV POSTGRESQL_JDBC_VERSION=42.7.1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Install Poetry and dependencies
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-root

# ... (inside Download Debezium JARs section)
RUN mkdir -p /app/debezium/lib && cd /app/debezium/lib && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-api/${DEBEZIUM_VERSION}/debezium-api-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-core/${DEBEZIUM_VERSION}/debezium-core-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-embedded/${DEBEZIUM_VERSION}/debezium-embedded-${DEBEZIUM_VERSION}.jar && \
    # ADD THIS ONE:
    wget https://repo1.maven.org/maven2/io/debezium/debezium-engine/${DEBEZIUM_VERSION}/debezium-engine-${DEBEZIUM_VERSION}.jar && \
    # ADD THESE FOR STORAGE:
    wget https://repo1.maven.org/maven2/io/debezium/debezium-storage-file/${DEBEZIUM_VERSION}/debezium-storage-file-${DEBEZIUM_VERSION}.jar && \
    # Debezium 2.5.0.Final components
    wget https://repo1.maven.org/maven2/io/debezium/debezium-api/${DEBEZIUM_VERSION}/debezium-api-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-core/${DEBEZIUM_VERSION}/debezium-core-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-embedded/${DEBEZIUM_VERSION}/debezium-embedded-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}.jar && \
    # Kafka Connect 3.6.0 components
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CONNECT_VERSION}/kafka-clients-${KAFKA_CONNECT_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/connect-api/${KAFKA_CONNECT_VERSION}/connect-api-${KAFKA_CONNECT_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/connect-runtime/${KAFKA_CONNECT_VERSION}/connect-runtime-${KAFKA_CONNECT_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/connect-json/${KAFKA_CONNECT_VERSION}/connect-json-${KAFKA_CONNECT_VERSION}.jar && \
    \
    # Other Dependencies (JDBC Driver, Logging, Jackson)
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar && \
    wget https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.9/slf4j-simple-2.0.9.jar && \
    wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.3/jackson-databind-2.15.3.jar && \
    wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.3/jackson-core-2.15.3.jar && \
    wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.3/jackson-annotations-2.15.3.jar

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Create required directories
RUN mkdir -p /app/data/offsets /app/data/history /app/logs

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Expose metrics port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Use entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
