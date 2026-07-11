FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        build-essential \
        postgresql-client \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV RETAIL_PROJECT_ROOT=/opt/retail-lakehouse

USER airflow

COPY requirements.txt requirements-dbt.txt requirements-airflow.txt /tmp/
RUN pip install --no-cache-dir \
    -r /tmp/requirements-airflow.txt \
    -r /tmp/requirements.txt \
    -r /tmp/requirements-dbt.txt

WORKDIR /opt/airflow
