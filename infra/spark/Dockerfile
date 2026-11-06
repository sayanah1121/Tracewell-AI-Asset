# Spark 3.5 on Python 3.11, the same Python as the Airflow image, so a driver
# running in Airflow and executors running here agree on the Python version.
FROM eclipse-temurin:17-jre AS jre

FROM python:3.11-slim-bookworm

COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/java/openjdk/bin:${PATH}

RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.5.8
RUN pip install --no-cache-dir --default-timeout=600 --retries 10 pyspark==${SPARK_VERSION}

ARG ICEBERG_VERSION=1.12.0
ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/iceberg-jars/
ADD ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/iceberg-jars/

# Kafka (Redpanda) + Avro connectors for publishing risk updates.
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark-extra-jars/

RUN pip install --no-cache-dir --default-timeout=600 --retries 10 pytest==8.4.2

ENV SPARK_CONF_DIR=/opt/spark-conf
ENV PYSPARK_PYTHON=python3
COPY spark-defaults.conf /opt/spark-conf/spark-defaults.conf

WORKDIR /opt/tracewell
