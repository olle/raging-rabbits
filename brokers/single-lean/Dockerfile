# Lean broker: base image, no management console.
# Machine-readable surfaces only: Prometheus metrics + internal event exchange.
FROM rabbitmq:4.3

RUN rabbitmq-plugins enable --offline \
      rabbitmq_management_agent \
      rabbitmq_prometheus \
      rabbitmq_event_exchange
