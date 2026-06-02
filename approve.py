import json
import time
from kafka import KafkaConsumer, KafkaProducer


def safe_json(v):
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        return None


# Actions allowed to run automatically. Anything else needs a human.
AUTO_OK = {"log_only"}

consumer = KafkaConsumer(
    "agent.audit",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="approver",
    value_deserializer=safe_json,
    consumer_timeout_ms=5000,
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

approved, rejected, auto = 0, 0, 0

for msg in consumer:
    record = msg.value
    if not isinstance(record, dict) or "agent_decision" not in record:
        continue

    decision = record["agent_decision"]
    event = record["input_event"]
    action = decision["recommended_action"]

    # low-risk actions don't need a human
    if action in AUTO_OK:
        producer.send("agent.actions", {
            "ts": int(time.time() * 1000),
            "action": action, "event": event,
            "approved_by": "auto", "status": "executed",
        })
        auto += 1
        continue

    # everything else: stop and ask a human
    print("\n" + "=" * 60)
    print(f"  AGENT WANTS TO ACT")
    print(f"  device:   {event['device']}  ({event['temp_c']}C)")
    print(f"  severity: {decision['severity'].upper()}")
    print(f"  action:   {action}")
    print(f"  reason:   {decision['reason']}")
    print("=" * 60)
    answer = input("  Approve this action? (y/n): ").strip().lower()

    if answer == "y":
        producer.send("agent.actions", {
            "ts": int(time.time() * 1000),
            "action": action, "event": event,
            "approved_by": "human", "status": "executed",
        })
        approved += 1
        print("  -> APPROVED. Action sent to agent.actions.")
    else:
        producer.send("agent.actions", {
            "ts": int(time.time() * 1000),
            "action": action, "event": event,
            "approved_by": "human", "status": "rejected",
        })
        rejected += 1
        print("  -> REJECTED. Logged, not executed.")

producer.flush()
print(f"\ndone — {approved} approved, {rejected} rejected, {auto} auto-run")
