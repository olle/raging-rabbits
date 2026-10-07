package example.raging_rabbits;

import java.util.List;
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
 * Declares the baseline topology — one queue per client, each with several topic bindings
 * plus one binding per fanout exchange — then returns so the app simply idles (no listeners,
 * so the only load on the machine/broker is the declared topology itself).
 */
@Component
@Order(10)
public class ClientTopologyProvisioner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(ClientTopologyProvisioner.class);

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
    int topicBindings = Math.max(0, props.getTopicBindings());
    List<String> fanoutNames =
        props.getFanoutExchanges().stream().filter(n -> n != null && !n.isBlank()).toList();
    int concurrency = Math.max(1, props.getConcurrency());
    int logEvery = Math.max(1, props.getLogEvery());

    TopicExchange topic = new TopicExchange(props.getExchange(), true, false);
    rabbitAdmin.declareExchange(topic);
    List<FanoutExchange> fanouts =
        fanoutNames.stream().map(n -> new FanoutExchange(n, true, false)).toList();
    fanouts.forEach(rabbitAdmin::declareExchange);

    long totalObjects = (long) clients * (1 + topicBindings + fanouts.size());
    int width = Math.max(6, String.valueOf(clients).length());
    log.info(
        "Declaring topology: {} clients x (1 queue + {} topic bindings + {} fanout bindings) = {} objects"
            + " (topic '{}', fanouts {}, {} threads). First queue: '{}{}'.",
        clients,
        topicBindings,
        fanouts.size(),
        totalObjects,
        topic.getName(),
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
              declareOne(n, width, topic, fanouts, topicBindings);
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
        (long) clients * topicBindings,
        (long) clients * fanouts.size(),
        String.format("%.1f", totalSec),
        String.format("%.0f", totalObjects / Math.max(totalSec, 0.001)));
  }

  private void declareOne(
      int n, int width, TopicExchange topic, List<FanoutExchange> fanouts, int topicBindings) {
    String id = pad(n, width);
    // Classic queue: durable, non-exclusive, non-auto-delete. No args -> lightest possible.
    Queue queue = new Queue(props.getQueuePrefix() + id, props.isDurable(), false, false);
    rabbitAdmin.declareQueue(queue);
    for (int k = 1; k <= topicBindings; k++) {
      rabbitAdmin.declareBinding(
          BindingBuilder.bind(queue).to(topic).with(props.getRoutingKeyPrefix() + id + ".s" + k));
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
