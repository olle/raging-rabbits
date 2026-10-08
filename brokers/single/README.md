# `single` — one RabbitMQ node

Baseline stack: a single RabbitMQ node with the management UI.
Used for client-topology scale tests (queue/binding counts).

## Run

```bash
make up-single
# or raw: docker compose -f brokers/single/compose.yaml up -d
```

Management UI: http://localhost:15672 (guest/guest). AMQP: `localhost:5672`.

Fresh reset (deletes all queues/exchanges and the container volume):

```bash
make reset-single
```

Broker credentials: RabbitMQ 4.x images ship with no default user, so the
compose file creates one on first boot via `RABBITMQ_USER` /
`RABBITMQ_PASSWORD` (defaults `guest`/`guest`, matching the app defaults).

## Point the app at it

```bash
make single            # CLIENTS=20000 default
make single CLIENTS=5000
```

(`SPRING_RABBITMQ_HOST`/`SPRING_RABBITMQ_PORT` default to `localhost:5672`,
so no extra config is needed for this stack.)

## Verify

Queue count and per-exchange binding counts via the management API:

```bash
curl -s -u guest:guest "http://localhost:15672/api/queues?page=1&page_size=1" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['total_count'])"
curl -s -u guest:guest "http://localhost:15672/api/exchanges/%2F/clients.topic/bindings/source?page=1&page_size=1" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['total_count'] if isinstance(d, dict) else len(d))"
```

Expected for the default baseline (`CLIENTS=20000`, 6 contexts × 2 keys + 3 fanouts):
20000 queues, 40000 bindings on each `<context>.events` exchange, 20000 on each
`broadcast.*` exchange.
