# Use official PySpark image as base
FROM apache/spark-py:latest

USER root
# Install pandas and any other Python packages you want
RUN pip install --no-cache-dir pandas

