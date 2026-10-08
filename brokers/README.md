# Broker architectures

Each subdirectory is a self-contained broker setup: its own `compose.yaml`,
`README.md`, `.env.example`, and init scripts. Nothing cross-imports.
Run a stack from its folder:

```bash
docker compose -f brokers/<arch>/compose.yaml up -d
```

## Architectures

| Folder | Status | Description |
|---|---|---|
| [`single/`](single/) | ready | One RabbitMQ node with management UI. Baseline for scale tests. |
| [`single-lean/`](single-lean/) | ready | One node, no console: Prometheus metrics + event exchange only. |
| [`federated-edges/`](federated-edges/) | planned | 3-node leader quorum cluster + single-node edges with full-replication exchange federation + HAProxy. |
| [`quorum-cycler/`](quorum-cycler/) | planned | 3-node quorum cluster + per-node sidecars doing sequential wipe-and-rejoin cycles. |

## Port map

Stacks are designed to run side by side. App endpoints use
`SPRING_RABBITMQ_HOST` / `SPRING_RABBITMQ_PORT` (see root README).

| Stack | AMQP | Management / other |
|---|---|---|
| `single` | 5672 | 15672 |
| `single-lean` | 5681 | 15692 (Prometheus) |
| `federated-edges` leader ×3 | 5673–5675 | 15673–15675 |
| `federated-edges` edge-a / edge-b | 5676 / 5677 | 15676 / 15677 |
| `federated-edges` haproxy | 5680 (AMQP) | 8404 (stats) |
| `quorum-cycler` rabbit1–3 | 5691–5693 | 15691–15693 |
| `quorum-cycler` sidecars | — | 8091–8093 (status/metrics) |
