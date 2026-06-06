# sentinel-redpanda

A small streaming pipeline on Redpanda where an AI agent triages anomalies in a
sensor stream, but can't take any risky action without a human approving it first.

Built as a weekend project. The point isn't the agent (that part's easy), it's
the stuff around it: every decision gets logged, and anything dangerous waits on
a human.

## Flow

```
producer    ->  telemetry.raw
routing     ->  splits into telemetry.clean / telemetry.anomalies
agent.py    ->  reads anomalies, LLM decides severity + action, logs it to agent.audit
approve.py  ->  reads decisions, asks a human y/n for risky ones, writes agent.actions
```

Five topics: `telemetry.raw`, `telemetry.clean`, `telemetry.anomalies`, `agent.audit`, `agent.actions`.

## Two versions of some pieces

I built a couple of the stages two ways, to compare a hand-rolled approach against the platform-native one:

- **Routing:** `route.py` (hand-rolled Python consumer/producer) and `pipeline.yaml` (Redpanda Connect, config-only — no code). Connect also preserves message lineage automatically (source topic, offset, partition).
- **Producer:** `produce.py` (Python) and `produce.cpp` (C++ with librdkafka, for higher throughput on the hot path).

## Run it

Start Redpanda:

```
docker run -d --name redpanda -p 9092:9092 -p 9644:9644 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --mode dev-container --smp 1 \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092
```

```
pip install kafka-python-ng anthropic
export ANTHROPIC_API_KEY=your-key

./reset.sh          # create/clear topics
./run.sh            # produce -> route -> agent (python versions)
python approve.py   # human approval step
```

Run routing via Connect instead of the Python script:

```
docker run --rm --network host \
  -v $(pwd)/pipeline.yaml:/connect.yaml \
  docker.redpanda.com/redpandadata/connect run /connect.yaml
```

Build and run the C++ producer instead of produce.py:

```
clang++ -std=c++17 produce.cpp -o produce_cpp \
  -I$(brew --prefix librdkafka)/include \
  -L$(brew --prefix librdkafka)/lib \
  -lrdkafka++ -lrdkafka
./produce_cpp
```

## Notes

- The agent calls Claude to triage each anomaly. The model can return malformed output, so `agent.py` strips markdown fences and wraps the whole call in a try/except — if it still can't parse a response, it escalates that reading for human review instead of crashing the pipeline.
- Routing (both versions) guards against missing/malformed fields instead of assuming every message is well-formed.
- `approve.py` auto-runs harmless actions and only stops to ask on the risky ones, so you're not rubber-stamping a hundred log entries.

## Stuff I'd add with more time

- run continuously + a small dashboard for throughput / latency
- schema enforcement on the topics
