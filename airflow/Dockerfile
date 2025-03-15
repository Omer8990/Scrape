FROM apache/airflow:2.7.1-python3.9

USER root

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
