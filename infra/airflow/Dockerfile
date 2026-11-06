# Airflow 3 + Java 17 + PySpark, so SparkSubmitOperator can run spark-submit (client mode) from here.
FROM eclipse-temurin:17-jre AS jre

FROM apache/airflow:3.3.2-python3.11

USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/java/openjdk/bin:${PATH}

ARG ICEBERG_VERSION=1.12.0
ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/iceberg-jars/
ADD ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/iceberg-jars/
RUN chmod -R a+r /opt/iceberg-jars

USER airflow
ARG SPARK_VERSION=3.5.8
# confluent-kafka: flink_job_health reads the consumer group's committed offsets to measure Kafka lag
RUN pip install --no-cache-dir --default-timeout=600 --retries 10 "apache-airflow==3.3.2" apache-airflow-providers-apache-spark "pyspark==${SPARK_VERSION}"     "confluent-kafka==2.16.0"

ENV SPARK_CONF_DIR=/opt/spark-conf
ENV PYSPARK_PYTHON=python3

USER root
ARG MAVEN=https://repo1.maven.org/maven2
# Kafka (Redpanda) + Avro connectors for publishing risk updates.
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark-extra-jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark-extra-jars/
RUN chmod -R a+r /opt/spark-extra-jars
USER airflow
