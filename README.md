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
  (one queue per client, 7 topic + 3 fanout bindings by default),
  then idles with zero consumers so the only load is the topology itself.

## Quickstart

```bash
docker compose -f brokers/single/compose.yaml up -d
CLIENTS=20000 ./mvnw spring-boot:run
```

## App configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `CLIENTS` | `20000` | Client queues to create |
| `TOPIC_BINDINGS` | `7` | Topic bindings per queue (keys `<prefix><id>.s1..sN`) |
| `FANOUT_EXCHANGES` | `broadcast.alpha,broadcast.beta,broadcast.gamma` | Shared fanout exchanges (one binding per queue each) |
| `CONCURRENCY` | `8` | Parallel declaration threads |
| `EXCHANGE` | `clients.topic` | Shared topic exchange |
| `SPRING_RABBITMQ_HOST` / `SPRING_RABBITMQ_PORT` | `localhost` / `5672` | Broker endpoint |
| `LOG_EVERY` | `1000` | Progress logging interval (clients) |

Baseline result on a dev machine: 20000 clients (20000 queues + 200000
bindings) declared in ~2.5 min; app idles at ~120MB RSS while the broker
holds ~2.7GB of queue metadata — broker RAM is the binding constraint.

## Tests

```bash
./mvnw test
```

(The suite pins a tiny deterministic topology; it needs a broker on
`localhost:5672`, i.e. the `single` stack.)
