FROM apache/spark:4.0.1

USER root

WORKDIR /app

COPY spark/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy Iceberg/S3 jars into Spark's own classpath
COPY spark/jars/*.jar /opt/spark/jars/

COPY spark/conf/ /opt/spark/conf/

ENV SPARK_HOME=/opt/spark

CMD ["tail", "-f", "/dev/null"]