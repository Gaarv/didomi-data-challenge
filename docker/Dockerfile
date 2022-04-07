FROM ubuntu:20.04

ARG JDK_VERSION=11
RUN apt-get update && \
    apt-get install -qqy --no-install-recommends curl build-essential openjdk-${JDK_VERSION}-jdk \
    python3-dev python3-pip python3-venv python3-wheel && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64
ENV VIRTUAL_ENV=/app/venv

ENV SPARK_HOME=/opt/spark-3.2.1-bin-hadoop3.2
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
ENV PYSPARK_PYTHON=$VIRTUAL_ENV

RUN curl -k -o spark-3.2.1-bin-hadoop3.2.tgz https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
RUN tar zxf spark-3.2.1-bin-hadoop3.2.tgz --directory /opt

RUN mkdir -p /app
WORKDIR /app

RUN pip3 install wheel && \
    python3 -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY setup.py setup.py
COPY didomi_spark didomi_spark
RUN pip install .