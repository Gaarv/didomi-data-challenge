FROM python:3.9-slim

ENV JDK_VERSION=11
ENV JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64
RUN apt-get update && \
    apt-get install -qqy --no-install-recommends make openjdk-${JDK_VERSION}-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY setup.py setup.py
COPY didomi_spark didomi_spark

RUN pip install .