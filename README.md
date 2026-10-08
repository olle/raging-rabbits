# raging-rabbits

Scale and resilience experiments for RabbitMQ-backed client topologies:
tens of thousands of client queues with topic + fanout bindings, live
messaging noise, and drain-only consumers — plus the broker architectures
to serve them.

The baseline scenario this repo proves: **~20,000 individual clients**,
each bound to **5–7 topic exchanges** (bounded contexts, 1–3 routing keys
each) plus shared **fanout exchanges** that every client subscribes to.

## How a run works

Each run goes through sequenced phases, all visible on the live status screen:

1. **Provision** — declares the topology: one queue per client (`client.000001…`),
   1–3 routing keys on every bounded-context topic exchange, one binding per
   fanout exchange. Parallel declaration threads; then the app would idle.
2. **Drain attach** — `CONSUMERS` virtual-thread workers each open their own
   connection and attach one auto-ack consumer per queue in their shard.
   Attaching happens *before* any traffic flows.
3. **Noise** (optional, `NOISE=off` by default) — steady paced publishing
   round-robin across all topic exchanges and fanouts. Every topic message
   reuses the exact binding-key formula, so each one routes into a real queue.
4. **Drain** — consumers ACK on delivery with no handling and no per-message
   logging (only a counter), keeping queue depths — and broker memory — bounded.

## Topology model

- **Topic exchanges:** one per bounded context (DDD), named `<context>.events`
  (default contexts: `orders`, `payments`, `shipping`, `notifications`,
  `billing`, `inventory`). Each client binds `KEYS` routing keys per exchange,
  shaped `client.<id>.<subject>` with realistic per-context subjects
  (`order.created`, `payment.captured`, …). Subjects rotate per client
  (`(client + k) % subjects`), so bindings spread evenly — e.g. 400 bindings
  on an exchange split 134/133/133 across its subjects.
- **Fanout exchanges:** shared broadcasts every client receives
  (`broadcast.announcements`, `broadcast.alerts`, `broadcast.config`),
  one binding per queue each.
- **Object math (defaults):** 1 queue + 6 contexts × 2 keys + 3 fanouts =
  **16 objects per client** → 320,000 at 20,000 clients.
  Worst case (`TOPICS` with 7 contexts, `KEYS=3`) is 22 per client.

## Quickstart

```bash
make run-single                 # starts brokers/single + runs CLIENTS=20000
make run-single CLIENTS=5000 NOISE=low CONSUMERS=5
```

`make help` shows the available targets. The app idles after provisioning,
so the run blocks until you stop it (Ctrl-C); the broker keeps its topology.

## Makefile targets

| Target | What it does |
|---|---|
| `help` (default) | Show available targets |
| `run-single` | Start the `single` stack, wait for AMQP, run the baseline app |

All baseline knobs below can be overridden per invocation, e.g.
`make run-single CLIENTS=500 NOISE=medium CONSUMERS=2`.

## Configuration reference

| Var | Default | Meaning |
|---|---|---|
| `CLIENTS` | `20000` | Client queues to create |
| `TOPICS` | `orders,payments,shipping,notifications,billing,inventory` | Bounded contexts, one topic exchange each |
| `TOPIC_SUFFIX` | `events` | Exchange suffix: `orders` → `orders.events` |
| `KEYS` | `2` | Routing keys per client per context exchange (1–3, clamped to max 3) |
| `FANOUTS` | `broadcast.announcements,broadcast.alerts,broadcast.config` | Shared fanout exchanges (one binding per queue each) |
| `CONCURRENCY` | `8` | Parallel topology-declaration threads |
| `NOISE` | `off` | `low` (5 msg/s), `medium` (25/s), `high` (100/s), or a custom msg/s number |
| `NOISE_TTL_MS` | `30000` | Per-message TTL for noise — it evaporates instead of filling queues |
| `CONSUMERS` | `1` | Drain workers (virtual threads); each drains an equal shard of the queues |
| `DRAIN_CHANNELS` | `4` | Channels per drain worker connection |
| `DURABLE` | `true` | Declare queues/exchanges durable |
| `QUEUE_PREFIX` | `client.` | Queue name prefix (`client.000001…`) |
| `ROUTING_KEY_PREFIX` | _(queue prefix)_ | Topic key prefix |
| `MONITOR` | `auto` | Status overview: `auto` (TTY-aware), `console` (force ANSI screen), `log` (summary lines) |
| `SPRING_RABBITMQ_HOST` / `SPRING_RABBITMQ_PORT` | `localhost` / `5672` | Broker endpoint |
| `SPRING_RABBITMQ_USERNAME` / `SPRING_RABBITMQ_PASSWORD` | `guest` / `guest` | Broker credentials |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | `guest` / `guest` | Broker-side user created on first boot (compose) |

## Example runs

```bash
# Smoke test: tiny topology, silent, fast
make run-single CLIENTS=50 MONITOR=log

# Default baseline: 20k clients, silent, 1 drain worker
make run-single

# Lively baseline: traffic + sharded drain
make run-single CLIENTS=2000 NOISE=low CONSUMERS=5

# Worst-case shape: 7 contexts, max keys, medium noise
make run-single CLIENTS=20000 TOPICS=orders,payments,shipping,notifications,billing,inventory,support \
  KEYS=3 NOISE=medium CONSUMERS=8
```

## Status screen

On a terminal the app redraws one block in place at 1 Hz (no scrolling);
piped output degrades to a summary line every 5s (`MONITOR=log` forces it,
`console` forces the screen):

```
phase    PROVISIONING topology
clients  [████████████────────] 908 / 1.500 (60%) · 3.681 obj/s · ETA 00:02
objects  14.528 / 24.000 declared
noise    waiting for provisioning…
drain    on · 5 workers · 1.500 queues · 5 conns · 748 consumed (5,0/s)
elapsed  00:04
```

Note on the `drain` row: it counts *deliveries*, so it runs hotter than the
`noise` publish rate — one fanout publish lands in every queue. Equal
deliveries-per-queue is the drain keeping up, not duplication. (Number
formatting follows the JVM locale.)

## Broker architectures

[`brokers/`](brokers/) holds one folder per broker setup, each self-contained
(compose file, README, init scripts). See
[`brokers/README.md`](brokers/README.md) for the index and the port map
(stacks are designed to run side by side).

- [`brokers/single/`](brokers/single/) — **ready.** One node + management UI.
  The baseline target. Includes verify one-liners and the expected
  queue/binding counts.
- [`brokers/single-lean/`](brokers/single-lean/) — **ready.** One node, no
  console: Prometheus metrics + internal event exchange instead of the
  management plugin. The machine-readable stack for CLI/TUI feedback.
- `brokers/federated-edges/` — **planned.** 3-node leader quorum cluster with
  full-replication exchange federation to single-node edges behind HAProxy.
- `brokers/quorum-cycler/` — **planned.** 3-node quorum cluster with per-node
  sidecars doing sequential wipe-and-rejoin cycles.

## Verify against the broker

```bash
# total queues
curl -s -u guest:guest "http://localhost:15672/api/queues?page=1&page_size=1" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['total_count'])"
# bindings on one context exchange
curl -s -u guest:guest "http://localhost:15672/api/exchanges/%2F/orders.events/bindings/source" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d))"
```

Expected for defaults at 20,000 clients: 20,000 queues, 40,000 bindings per
`<context>.events` exchange, 20,000 per `broadcast.*` exchange.
Management UI: http://localhost:15672 (guest/guest).

## Tests

```bash
./mvnw test
```

The suite pins a tiny deterministic topology (5 clients, 2 contexts, noise
off, 2 drain workers, log monitor) and needs a broker on `localhost:5672`.

## Findings so far

- **Baseline fits on a dev machine:** 20,000 clients / 320,000 objects declare
  in ~2.5 min; the app idles around ~120MB RSS.
- **Broker RAM is the binding constraint:** ~2.2/3.8GB container usage at
  15k×16 topology, with memory-alarm flaps during provisioning. Queue metadata,
  not traffic, sets the ceiling.
- **Drain scales via workers, not consumers:** one shared client connection
  deadlocks consumer registration past ~1,400 queues (delivery flood starves
  the Consume-Ok RPCs — attach must happen before publish, over separate
  physical connections). Hence virtual-thread workers with one connection each.
- **Noise is self-cleaning:** per-message TTL plus always-on drain keeps depths
  at ~0 even at full replication.

## Troubleshooting

- **Fresh stack rejects `guest/guest`:** RabbitMQ 4.x images ship with no
  default user — `brokers/single/compose.yaml` creates one from
  `RABBITMQ_USER`/`RABBITMQ_PASSWORD` on first boot. If you changed them,
  match `SPRING_RABBITMQ_USERNAME`/`PASSWORD` on the app side.
- **Broker won't boot / `enospc`:** anonymous mnesia volumes orphan on every
  reset; run `docker volume prune -f` (resets via compose use `down -v`).
- **Drain row stays `off`:** `CONSUMERS` must be a positive worker count;
  anything else warns and falls back to 1 worker — the warning echoes the
  received value.
- **`make` behaves oddly:** macOS ships GNU Make 3.81 — the Makefile avoids
  pattern rules + explicit `.PHONY` combinations that version mishandles.
