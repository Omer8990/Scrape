FROM apache/airflow:2.7.1-python3.9

USER root

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directories with proper ownership
RUN mkdir -p /opt/airflow/jars /opt/airflow/spark_scripts
# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/airflow/jars/ && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -o /opt/airflow/jars/postgresql-42.3.1.jar && \
    chmod 644 /opt/airflow/jars/postgresql-42.3.1.jar
# Download PostgreSQL JDBC driver using curl (which we installed above)
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -o /opt/airflow/jars/postgresql-42.3.1.jar

USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Adding the spark-submit thingi
RUN export PATH=$PATH:/opt/spark/bin