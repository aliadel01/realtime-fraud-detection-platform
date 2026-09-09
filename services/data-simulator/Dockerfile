FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY payment_gateway_producer.py .
COPY identity_risk_producer.py .
COPY hot_key_utils.py .

# overridden per-service in docker-compose
CMD ["python", "payment_gateway_producer.py"]