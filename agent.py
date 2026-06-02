import json
import time
from kafka import KafkaConsumer, KafkaProducer


def safe_json(v):
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        return None


# ---- the "brain" ---------------------------------------------------------
# Mock LLM: stands in for a real model call. Same inputs/outputs as the real
# thing, so swapping in Anthropic/OpenAI later is a tiny change.
def llm_triage(event):
    temp = event["temp_c"]
    if temp >= 125:
        severity = "critical"
        action = "shutdown_device"
        reason = f"{temp}C far exceeds safe limit; immediate shutdown advised."
    elif temp >= 110:
        severity = "high"
        action = "alert_operator"
        reason = f"{temp}C is well above normal; operator should investigate."
    else:
        severity = "medium"
        action = "log_only"
        reason = f"{temp}C is elevated but not critical; monitor."
    # simulate model latency so the metrics later look real
    time.sleep(0.1)
    return {"severity": severity, "recommended_action": action, "reason": reason}
# --------------------------------------------------------------------------


consumer = KafkaConsumer(
    "telemetry.anomalies",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="agent",
    value_deserializer=safe_json,
    consumer_timeout_ms=5000,
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

count = 0
for msg in consumer:
    event = msg.value
    if not isinstance(event, dict) or "temp_c" not in event:
        continue

    decision = llm_triage(event)

    # THE GOVERNANCE PART: every decision is recorded, in full, before
    # anything acts on it. Input, output, and timestamp — nothing off the record.
    audit_record = {
        "ts": int(time.time() * 1000),
        "input_event": event,
        "agent_decision": decision,
    }
    producer.send("agent.audit", audit_record)

    count += 1
    print(f"[{decision['severity'].upper():8}] {event['device']} "
          f"{event['temp_c']}C -> {decision['recommended_action']}")

producer.flush()
print(f"\ndone — {count} anomalies triaged and logged to agent.audit")
