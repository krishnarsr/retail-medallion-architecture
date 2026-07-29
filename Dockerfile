# Pin Debian Bookworm because Spark 3.5 is validated with Java 17 and the
# floating python:3.11-slim tag now resolves to Trixie, where OpenJDK 17 was
# removed from the package repository.
FROM python:3.11-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENTRYPOINT ["python", "-m", "src.pipeline"]
