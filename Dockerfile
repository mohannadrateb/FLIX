# Use official Airflow image as base
FROM apache/airflow:2.8.1

USER root

# Install Java (OpenJDK 17) and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark (choose your version)
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm /tmp/spark.tgz

# Set Spark env vars
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Switch back to airflow user
USER airflow

# Install Airflow Spark provider and pandas
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.0 \
    pandas

