
# Laboratorio 09 - CDC y Event Streaming

## Descripción
Implementación de una arquitectura de captura de cambios (CDC) desde PostgreSQL hacia una capa Bronze en S3 local.

## Arquitectura
PostgreSQL → Debezium → Kafka → Python Consumer → Floci S3 Bronze

## Tecnologías
- Docker Desktop
- PostgreSQL 17
- Apache Kafka 4.3.1
- Debezium 3.7
- Kafka UI
- Floci
- Python 3.12
- boto3
- Git y GitHub

## Estructura
- docker-compose.yml
- postgres/init.sql
- debezium/postgres-connector.json
- consumer/consumer.py
- consumer/requirements.txt
