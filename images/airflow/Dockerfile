FROM apache/airflow:3.1.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY images/airflow/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt