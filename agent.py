import json
import time
from kafka import KafkaConsumer, KafkaProducer
from anthropic import Anthropic

client = Anthropic()


def safe_json(v):
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        return None


def llm_triage(event):
    prompt = f"""You are triaging a sensor anomaly. Reading: {event}

Respond with ONLY a JSON object, no other text, in this exact format:
{{"severity": "critical|high|medium", "recommended_action": "shutdown_device|alert_operator|log_only", "reason": "one sentence"}}"""

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        return json.loads(raw)

    except Exception as e:
        # model failed or returned non-JSON -> escalate instead of crashing
        return {
            "severity": "high",
            "recommended_action": "alert_operator",
            "reason": f"triage failed, escalating for human review ({type(e).__name__})",
        }


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
