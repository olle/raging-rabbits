# `single` — one RabbitMQ node

Baseline stack: a single RabbitMQ node with the management UI.
Used for client-topology scale tests (queue/binding counts).

## Run

```bash
docker compose -f brokers/single/compose.yaml up -d
```

Management UI: http://localhost:15672 (guest/guest). AMQP: `localhost:5672`.

Fresh reset (deletes all queues/exchanges — data lives in the container):

```bash
docker compose -f brokers/single/compose.yaml down
docker compose -f brokers/single/compose.yaml up -d
```

## Point the app at it

```bash
CLIENTS=20000 ./mvnw spring-boot:run
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

Expected for the default baseline (`CLIENTS=20000`, 7 topic + 3 fanout bindings):
20000 queues, 140000 bindings on `clients.topic`, 20000 on each `broadcast.*` exchange.
