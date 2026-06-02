import json
from kafka import KafkaConsumer, KafkaProducer

THRESHOLD = 100  # temps above this are anomalies


def safe_json(v):
    # real streams have junk in them — skip anything that isn't valid JSON
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        return None


consumer = KafkaConsumer(
    "telemetry.raw",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   # read from the start
    group_id="router",
    value_deserializer=safe_json,
    consumer_timeout_ms=5000,        # stop after 5s of no new messages
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

clean, anomalies = 0, 0
for msg in consumer:
    event = msg.value
    if not isinstance(event, dict) or "temp_c" not in event:
        continue  # skip the "hello redpanda" line and any bad records
    if event["temp_c"] > THRESHOLD:
        producer.send("telemetry.anomalies", event)
        anomalies += 1
        print(f"ANOMALY -> {event}")
    else:
        producer.send("telemetry.clean", event)
        clean += 1

producer.flush()
print(f"\ndone — {clean} clean, {anomalies} anomalies")

