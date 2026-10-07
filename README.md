# raging-rabbits

Scale and resilience experiments for RabbitMQ-backed client topologies:
tens of thousands of client queues with topic + fanout bindings, and the
broker architectures to serve them.

## Layout

- [`brokers/`](brokers/) — broker setups, one folder per architecture.
  Start with [`brokers/single/`](brokers/single/) (one node).
  Planned: `federated-edges` (leader cluster + federated edges + HAProxy)
  and `quorum-cycler` (quorum cluster + wipe-and-rejoin sidecars).
  See [`brokers/README.md`](brokers/README.md) for the index and port map.
- `src/` — Spring Boot app. Declares the client topology
  (one queue per client, 1–3 routing keys on each bounded-context topic
  exchange plus one binding per shared fanout exchange),
  then idles with zero consumers so the only load is the topology itself.

## Quickstart

```bash
make baseline-single            # starts brokers/single + runs CLIENTS=20000
make baseline-single CLIENTS=5000
```

(`make help` lists all targets: per-architecture `up/down/reset/baseline`,
plus `build`, `test`, `clean`.)

## App configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `CLIENTS` | `20000` | Client queues to create |
| `TOPICS` | `orders,payments,shipping,notifications,billing,inventory` | Bounded contexts, one topic exchange (`<ctx>.events`) each |
| `KEYS` | `2` | Routing keys per client per context exchange (1–3, at most 3) |
| `FANOUTS` | `broadcast.announcements,broadcast.alerts,broadcast.config` | Shared fanout exchanges (one binding per queue each) |
| `CONCURRENCY` | `8` | Parallel declaration threads |
| `NOISE` | `off` | Messaging noise after provisioning: `low` (5/s), `medium` (25/s), `high` (100/s), or a custom msg/s number |
| `NOISE_TTL_MS` | `30000` | Per-message TTL for noise (self-cleaning) |
| `SPRING_RABBITMQ_HOST` / `SPRING_RABBITMQ_PORT` | `localhost` / `5672` | Broker endpoint |
| `LOG_EVERY` | `1000` | Progress logging interval (clients) |

Baseline result on a dev machine: 20000 clients × (1 queue + 6 contexts × 2 keys
+ 3 fanout bindings) = 320000 objects; app idles at ~120MB RSS while the broker
holds the queue metadata — broker RAM is the binding constraint. Worst case
(`TOPICS` with 7 contexts, `KEYS=3`) is 22 objects per client.

## Tests

```bash
./mvnw test
```

(The suite pins a tiny deterministic topology; it needs a broker on
`localhost:5672`, i.e. the `single` stack.)
