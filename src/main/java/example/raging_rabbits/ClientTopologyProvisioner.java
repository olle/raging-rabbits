package example.raging_rabbits;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Declares the baseline topology — one queue per client, each bound to every bounded-context
 * topic exchange with 1-3 routing keys plus one binding per fanout exchange — then returns so
 * the app simply idles (no listeners, so the only load on the machine/broker is the declared
 * topology itself).
 *
 * <p>Routing subjects rotate per client ({@code (clientIndex + k) % subjects}) so bindings
 * spread evenly across each context's keyspace instead of piling onto the same subjects.
 */
@Component
@Order(10)
public class ClientTopologyProvisioner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(ClientTopologyProvisioner.class);

  /** Bounded-context subjects; unknown contexts fall back to {@link #GENERIC_SUBJECTS}. */
  private static final Map<String, List<String>> SUBJECTS =
      Map.of(
          "orders", List.of("order.created", "order.shipped", "order.cancelled"),
          "payments", List.of("payment.authorized", "payment.captured", "payment.refunded"),
          "shipping", List.of("shipment.booked", "shipment.intransit", "shipment.delivered"),
          "notifications", List.of("notification.queued", "notification.sent", "notification.failed"),
          "billing", List.of("invoice.issued", "invoice.paid", "invoice.overdue"),
          "inventory", List.of("stock.reserved", "stock.released", "stock.low"),
          "support", List.of("ticket.opened", "ticket.escalated", "ticket.closed"));

  private static final List<String> GENERIC_SUBJECTS =
      List.of("evt.created", "evt.updated", "evt.closed");

  private final RagingClientsProperties props;
  private final RabbitAdmin rabbitAdmin;

  public ClientTopologyProvisioner(RagingClientsProperties props, RabbitAdmin rabbitAdmin) {
    this.props = props;
    this.rabbitAdmin = rabbitAdmin;
  }

  @Override
  public void run(ApplicationArguments args) {
    int clients = props.getClients();
    if (clients <= 0) {
      log.warn("app.clients={} (CLIENTS env), nothing to declare. App will idle.", clients);
      return;
    }
    List<String> contexts =
        props.getTopics().stream().filter(c -> c != null && !c.isBlank()).toList();
    int topicKeys = Math.min(3, Math.max(0, props.getKeys()));
    if (topicKeys != props.getKeys()) {
      log.warn(
          "app.keys={} clamped to {} (at most 3 routing keys per client per context exchange).",
          props.getKeys(),
          topicKeys);
    }
    List<String> fanoutNames =
        props.getFanouts().stream().filter(n -> n != null && !n.isBlank()).toList();
    int concurrency = Math.max(1, props.getConcurrency());
    int logEvery = Math.max(1, props.getLogEvery());

    String suffix = props.getTopicSuffix();
    List<TopicExchange> topics =
        contexts.stream()
            .map(c -> new TopicExchange(c + "." + suffix, true, false))
            .toList();
    topics.forEach(rabbitAdmin::declareExchange);
    List<FanoutExchange> fanouts =
        fanoutNames.stream().map(n -> new FanoutExchange(n, true, false)).toList();
    fanouts.forEach(rabbitAdmin::declareExchange);

    long bindingsPerClient = (long) contexts.size() * topicKeys + fanouts.size();
    long totalObjects = (long) clients * (1 + bindingsPerClient);
    int width = Math.max(6, String.valueOf(clients).length());
    log.info(
        "Declaring topology: {} clients x (1 queue + {} contexts x {} keys + {} fanout bindings) = {} objects"
            + " (topics {}, fanouts {}, {} threads). First queue: '{}{}'.",
        clients,
        contexts.size(),
        topicKeys,
        fanouts.size(),
        totalObjects,
        topics.stream().map(TopicExchange::getName).toList(),
        fanoutNames,
        concurrency,
        props.getQueuePrefix(),
        pad(1, width));

    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    AtomicInteger done = new AtomicInteger();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    long start = System.nanoTime();

    for (int i = 1; i <= clients && failure.get() == null; i++) {
      final int n = i;
      pool.submit(
          () -> {
            try {
              declareOne(n, width, contexts, topics, topicKeys, fanouts);
              int d = done.incrementAndGet();
              if (d == clients || d % logEvery == 0) {
                synchronized (log) {
                  double elapsedSec = (System.nanoTime() - start) / 1_000_000_000.0;
                  log.info(
                      "Progress: {}/{} clients ({} objects/s)",
                      d,
                      clients,
                      String.format("%.0f", totalObjects * d / clients / Math.max(elapsedSec, 0.001)));
                }
              }
            } catch (RuntimeException e) {
              failure.compareAndSet(null, e);
            }
          });
    }
    pool.shutdown();
    try {
      if (!pool.awaitTermination(4, TimeUnit.HOURS)) {
        pool.shutdownNow();
        throw new IllegalStateException("Topology declaration timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      pool.shutdownNow();
      throw new IllegalStateException("Topology declaration interrupted", e);
    }
    if (failure.get() != null) {
      throw new IllegalStateException("Topology declaration failed", failure.get());
    }

    double totalSec = (System.nanoTime() - start) / 1_000_000_000.0;
    log.info(
        "Topology ready: {} clients ({} queues, {} topic bindings, {} fanout bindings) in {}s ({} objects/s)."
            + " Idling (no consumers running).",
        clients,
        clients,
        (long) clients * contexts.size() * topicKeys,
        (long) clients * fanouts.size(),
        String.format("%.1f", totalSec),
        String.format("%.0f", totalObjects / Math.max(totalSec, 0.001)));
  }

  private void declareOne(
      int n,
      int width,
      List<String> contexts,
      List<TopicExchange> topics,
      int topicKeys,
      List<FanoutExchange> fanouts) {
    String id = pad(n, width);
    // Classic queue: durable, non-exclusive, non-auto-delete. No args -> lightest possible.
    Queue queue = new Queue(props.getQueuePrefix() + id, props.isDurable(), false, false);
    rabbitAdmin.declareQueue(queue);
    for (int c = 0; c < contexts.size(); c++) {
      List<String> subjects =
          new ArrayList<>(SUBJECTS.getOrDefault(contexts.get(c), GENERIC_SUBJECTS));
      for (int k = 0; k < topicKeys; k++) {
        String subject = subjects.get((n + k) % subjects.size());
        rabbitAdmin.declareBinding(
            BindingBuilder.bind(queue)
                .to(topics.get(c))
                .with(props.getRoutingKeyPrefix() + id + "." + subject));
      }
    }
    for (FanoutExchange fanout : fanouts) {
      rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(fanout));
    }
  }

  private static String pad(int i, int width) {
    String s = Integer.toString(i);
    if (s.length() >= width) {
      return s;
    }
    return "0".repeat(width - s.length()) + s;
  }
}
