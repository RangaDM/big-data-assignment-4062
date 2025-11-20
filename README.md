# Kafka Order Processing System (Python)

A Kafka-based streaming system using Python, Avro, Schema Registry, retry logic, running averages, and a Dead Letter Queue (DLQ).

---

## Features

- Avro serialization with Schema Registry  
- Real-time running average of order prices  
- Automatic retry (3 attempts) using Kafka headers  
- Dead Letter Queue for permanent failures  
- DLQ viewer with formatted output  
- Kafka KRaft mode (no ZooKeeper)

---

## Requirements

- Python 3.9+
- Docker & Docker Compose
- Confluent Schema Registry (via docker-compose)

---

## Quick Start

```bash
python -m venv venv
source venv/bin/activate   # macOS/Linux
# OR
venv\Scripts\activate      # Windows

pip install -r requirements.txt

docker-compose up -d
python create_topics.py
python producer/producer.py
python consumer/consumer.py
python dlq_consumer.py
```

---

## Web Interface

For a clearer view, after successfully running the application, visit:

[http://localhost:8080](http://localhost:8080)
