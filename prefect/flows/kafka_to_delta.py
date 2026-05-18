# prefect/flows/kafka_to_delta.py
from prefect import flow, task
from kafka import KafkaConsumer
import json, os
import pandas as pd
from datetime import datetime

@task
def consume_and_process():
    """Consume data from Kafka topic"""
    consumer = KafkaConsumer(
        "data.raw",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode())
    )
    records = []
    for msg in consumer:
        records.append(msg.value)

    print(f"Consumed {len(records)} records from Kafka")
    return records

@task
def save_to_delta(records):
    """Save records to Delta Lake (parquet format)"""
    if not records:
        print("No records to save")
        return
    
    df = pd.DataFrame(records)
    # Giả lập Delta Lake bằng parquet (local volume)
    path = "/opt/delta-lake/raw"
    os.makedirs(path, exist_ok=True)
    df.to_parquet(f"{path}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
    print(f"Saved {len(df)} records to Delta Lake")

@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow():
    """Main flow: consume from Kafka and save to Delta Lake"""
    records = consume_and_process()
    save_to_delta(records)

if __name__ == "__main__":
    # Deploy flow to Prefect Orion
    kafka_to_delta_flow.deploy(
        name="kafka-to-delta",
        work_pool_name="lab28-pool",
        cron="* */5 * * *"
    )
