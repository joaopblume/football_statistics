# Custom Spark image used by the ephemeral per-run Silver/Gold jobs.
#
# Bakes the Iceberg + S3A jars into the image so each `docker run --rm` job
# starts immediately instead of re-resolving packages via ivy every time, and
# installs Great Expectations for in-job data-quality validation.
#
# Build:  docker build -t football-spark:latest infra/spark
FROM jupyter/pyspark-notebook:latest

USER root
ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
RUN cd /usr/local/spark/jars && \
    curl -fSL -O "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" && \
    curl -fSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSL -O "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

USER ${NB_UID}
# Great Expectations (in-job validation) + s3fs for Data Docs to MinIO.
RUN pip install --no-cache-dir "great-expectations==1.3.13" "s3fs"
