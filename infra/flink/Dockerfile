FROM flink:1.20.3-scala_2.12-java17

ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.3.6
ARG MAVEN=https://repo1.maven.org/maven2

# Iceberg (Flink runtime + AWS S3FileIO) and the Hadoop client classes Iceberg needs.
# The Kafka connector is bundled in each job jar (flink-jobs/tracewell-stream), not installed here: the shaded
# flink-sql-connector-kafka would relocate Kafka classes and clash with DataStream jobs that read keys.
ADD --chown=flink:flink ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD --chown=flink:flink ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD --chown=flink:flink ${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar /opt/flink/lib/
ADD --chown=flink:flink ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar /opt/flink/lib/

# S3 filesystem plugin for checkpoints (s3://checkpoints/...).
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
 && chown -R flink:flink /opt/flink/plugins/s3-fs-hadoop
