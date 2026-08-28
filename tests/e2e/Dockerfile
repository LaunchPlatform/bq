FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE ./
COPY bq ./bq
RUN pip install --no-cache-dir -e ".[metrics]" "docker>=7,<8" "httpx>=0.27,<1"
COPY tests/e2e ./tests/e2e

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
