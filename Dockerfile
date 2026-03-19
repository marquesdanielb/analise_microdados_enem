FROM bitnami/spark:3.4.1

USER root
# Install necessary Python packages
RUN pip install --no-cache-dir pyspark mysql-connector-python pandas

# Download MySQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar -o /opt/spark/jars/mysql-connector-java-8.0.30.jar

WORKDIR /app
CMD ["spark-submit", "--jars", "/opt/spark/jars/mysql-connector-java-8.0.30.jar", "etl.py"]
