import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

sensor_ids = ["SENSOR_001", "SENSOR_002", "SENSOR_003"]

try:
    print(f"Enviando datos a Kafka: {KAFKA_TOPIC}", flush=True)

    while True:
        sensor_id = random.choice(sensor_ids)

        event = {
            "sensor_id": sensor_id,
            "temperatura": round(random.uniform(20, 38), 2),
            "humedad": round(random.uniform(30, 90), 2),
            "calidad_del_aire": random.randint(0, 150),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        metadata = producer.send(KAFKA_TOPIC, event).get(timeout=10)

        print(
            f"Mensaje enviado | sensor={sensor_id} "
            f"| particion={metadata.partition} "
            f"| offset={metadata.offset} | datos={event}",
            flush=True,
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("Productor detenido por el usuario.", flush=True)

finally:
    producer.flush()
    producer.close()
