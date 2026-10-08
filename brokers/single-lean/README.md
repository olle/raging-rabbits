# `single-lean` — one RabbitMQ node, no console

Same broker as [`single/`](../single/), minus the management plugin. There is
no browser UI and no HTTP API on 15672 — the machine-readable surfaces are:

- **Prometheus metrics** on `localhost:15692/metrics` (no auth by default):
  per-queue depths, publish/deliver rates, consumer counts, connections,
  channels, alarms. This is the polling feed for CLIs/TUIs.
- **Internal event exchange** `amq.rabbitmq.event` (topic, in `/`): live
  `queue.created/deleted`, `consumer.created/deleted`, `connection.*`,
  `alarm.set/cleared`, … messages. This is the push feed for CLIs/TUIs.
- **Local CLIs** via `docker exec` (`rabbitmqctl`, `rabbitmq-diagnostics`,
  `rabbitmq-queues`) for actions: purge, delete, rebalance, health checks.

What you lose vs `single/`: the HTTP API itself and the browser UI that rides
on it. Note that `rabbitmqadmin` v2 is a standalone binary (separate download,
not bundled with the broker) — it works against any node with the HTTP API
enabled, so it stays available on `single/`; on `single-lean/` use
`rabbitmqctl` / `rabbitmq-diagnostics` via `docker exec` instead.

## Run

```bash
docker compose -f brokers/single-lean/compose.yaml up -d --build
```

No `make` target (the Makefile is pruned to `help` + `run-single`); use raw
compose. AMQP is on `5681` so this stack runs beside `single/`.

Fresh reset:

```bash
docker compose -f brokers/single-lean/compose.yaml down -v
docker compose -f brokers/single-lean/compose.yaml up -d
```

## Point the app at it

```bash
CLIENTS=20000 SPRING_RABBITMQ_PORT=5681 ./mvnw -q spring-boot:run
```

## Verify

```bash
# no console
(nc -z localhost 15672 && echo "15672 OPEN (unexpected)") || echo "15672 closed (expected)"
# prometheus metrics
curl -s localhost:15692/metrics | grep -E "^rabbitmq_queues |^rabbitmq_queue_messages "
# event exchange exists
docker exec single-lean-rabbitmq-1 rabbitmqctl list_exchanges name type | grep amq.rabbitmq.event
# live event stream (Ctrl-C to stop)
docker exec single-lean-rabbitmq-1 rabbitmq-diagnostics consume_event_stream | head -5
```
