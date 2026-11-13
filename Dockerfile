# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}" \
    HF_HOME=/home/app/.cache/huggingface \
    CHROMA_HOST=chroma \
    CHROMA_PORT=8000 \
    KAFKA_BOOTSTRAP=kafka:9092 \
    TOKENIZERS_PARALLELISM=false \
    OMP_NUM_THREADS=2 \
    SPARK_LOCAL_IP=127.0.0.1

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app && useradd --system --gid app --create-home app
WORKDIR /app
COPY requirements.txt .
# Install in the final filesystem to avoid keeping a second full dependency
# tree in an intermediate build stage on disk-constrained laptops.
RUN python -m pip install torch==2.6.0 --index-url https://download.pytorch.org/whl/cpu \
    && python -m pip install -r requirements.txt \
    && python -m pip check
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*
COPY --chown=app:app *.py ./
RUN mkdir -p /app/data /app/runs /home/app/.cache/huggingface /home/app/.ivy2 \
    && chown -R app:app /app /home/app
USER app

ENTRYPOINT ["spark-submit", "--master", "local[2]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "stream_job.py"]
