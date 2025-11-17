# Kafka Order System (Python) — Assignment

## Features
- Avro serialization with Schema Registry
- Real-time running average of order prices
- Automatic retry (3 attempts) with header tracking
- Dead Letter Queue for permanent failures
- DLQ viewer with rich tables
- KRaft mode (no ZooKeeper)

## Quick Start
```bash
docker-compose up -d
python create_topics.py
python producer/producer.py
python consumer/consumer.py
python dlq_consumer.py