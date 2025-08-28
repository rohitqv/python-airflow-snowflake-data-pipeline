# Production Dockerfile for Data Pipeline
FROM apache/airflow:2.9.2-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jdk \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Download Spark JARs for Snowflake connectivity
RUN mkdir -p /opt/airflow/jars && \
    cd /opt/airflow/jars && \
    wget -q https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.22/snowflake-jdbc-3.13.22.jar && \
    wget -q https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.0-spark_3.2/spark-snowflake_2.12-2.11.0-spark_3.2.jar && \
    chown -R airflow:root /opt/airflow/jars

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application code
COPY --chown=airflow:root airflow/ /opt/airflow/
COPY --chown=airflow:root jobs/ /opt/airflow/jobs/
COPY --chown=airflow:root libs/ /opt/airflow/libs/
COPY --chown=airflow:root config/ /opt/airflow/config/
COPY --chown=airflow:root snowflake/ /opt/airflow/snowflake/

# Set Python path and Spark configuration
ENV PYTHONPATH="/opt/airflow:/opt/airflow/jobs:/opt/airflow/libs:${PYTHONPATH}"
ENV SPARK_CLASSPATH="/opt/airflow/jars/*"
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/airflow/jars/snowflake-jdbc-3.13.22.jar,/opt/airflow/jars/spark-snowflake_2.12-2.11.0-spark_3.2.jar pyspark-shell"

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
