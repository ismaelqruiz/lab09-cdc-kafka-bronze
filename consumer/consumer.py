
from kafka import KafkaConsumer
import boto3
import json
import time
from datetime import datetime, timezone

# Configuración
TOPIC = "ucv.public.sales"
BUCKET = "ucv-data-lake"
BATCH_SIZE = 5

# Consumidor Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
group_id=None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
)

# Cliente S3 local mediante Floci
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

# Verificar o crear bucket
try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"Bucket existente: {BUCKET}")
except Exception:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Bucket creado: {BUCKET}")

batch = []


# Guardar eventos en formato JSONL
def flush(records):
    if not records:
        return

    now = datetime.now(timezone.utc)

    key = (
        f"bronze/sales/year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"sales_cdc_{now:%Y%m%dT%H%M%S}_{int(time.time()*1000)}.jsonl"
    )

    body = "\n".join(
        json.dumps(record, ensure_ascii=False)
        for record in records
    ) + "\n"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    print(
        f"[S3] s3://{BUCKET}/{key} "
        f"({len(records)} eventos)"
    )


# Inicio del consumo
print(f"Escuchando topic {TOPIC}...")
print("Recuperando eventos desde el offset más antiguo disponible...")

try:
    for msg in consumer:

        payload = msg.value or {}
        cdc_data = payload.get("payload") or {}

        event = {
            "kafka_topic": msg.topic,
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "kafka_timestamp": msg.timestamp,
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        batch.append(event)

        print(
            f"topic={msg.topic} | "
            f"partition={msg.partition} | "
            f"offset={msg.offset} | "
            f"op={cdc_data.get('op')}"
        )

        if len(batch) >= BATCH_SIZE:
            flush(batch)
            batch.clear()

except KeyboardInterrupt:
    print("\nDeteniendo consumer...")

finally:
    try:
        flush(batch)
    finally:
        consumer.close()
        print("Consumer finalizado.")
