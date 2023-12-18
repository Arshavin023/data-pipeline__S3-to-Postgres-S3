ARG AIRFLOW_VERSION=latest-python3.8
FROM docker.io/apache/airflow:${AIRFLOW_VERSION}

FROM apache/airflow:latest-python3.8

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow
# Set the working directory in the container
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
COPY ./sql/transformation.sql /app/sql/transformation.sql
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt