# eventhub-to-kafka-bridge
Python program to read messages from Azure Event Hub and send it to Apache Kafka

## Requirements

- Python 3.10 or newer
- A reachable Apache Kafka broker. The bridge negotiates the Kafka protocol
  version at startup and exits with `KafkaTimeoutError` if no broker responds.

## Setup

1. Run pip install -r requirements.txt
2. Copy .env-sample to .env and edit your variables before running eventh_kafka_bridge.py
