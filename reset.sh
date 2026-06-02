#!/bin/bash
# Wipes all topics and recreates them empty — gives you a clean slate
# before recording a fresh demo take.

set -e

TOPICS=(telemetry.raw telemetry.clean telemetry.anomalies agent.audit agent.actions)

echo "Resetting topics..."
for t in "${TOPICS[@]}"; do
    docker exec redpanda rpk topic delete "$t" 2>/dev/null || true
    docker exec redpanda rpk topic create "$t" >/dev/null
    echo "  reset: $t"
done

echo "Done — all topics empty and ready."
