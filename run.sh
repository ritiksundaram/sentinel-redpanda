#!/bin/bash
# Runs the pipeline end to end: produce -> route -> triage.
# The human approval step (approve.py) is run separately, by hand,
# because it's interactive — that's the part you do live on camera.

set -e

echo "=== 1/3  Producing telemetry ==="
python produce.py

echo ""
echo "=== 2/3  Routing (clean vs anomalies) ==="
python route.py

echo ""
echo "=== 3/3  Agent triage -> audit log ==="
python agent.py

echo ""
echo "Pipeline done. Anomalies triaged and logged."
echo "Now run:  python approve.py   (the human-in-the-loop gate)"
