import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

devices = ["sensor-01", "sensor-02", "sensor-03", "sensor-04"]

for i in range(50):
    temp = round(random.uniform(60, 80), 1)        # normal range
    if random.random() < 0.15:                      # ~15% anomalies
        temp = round(random.uniform(110, 140), 1)   # way too hot

    event = {
        "device": random.choice(devices),
        "temp_c": temp,
        "ts": int(time.time() * 1000),
    }
    producer.send("telemetry.raw", event)
    print(f"sent: {event}")
    time.sleep(0.2)

producer.flush()
print("done — 50 events sent")
