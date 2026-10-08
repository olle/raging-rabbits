package example.raging_rabbits;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.DisposableBean;
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
 * Key formulas live in {@link ClientKeyspace}, shared with the noise publisher.
 */
@Component
@Order(10)
public class ClientTopologyProvisioner implements ApplicationRunner, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(ClientTopologyProvisioner.class);

  private final RagingClientsProperties props;
  private final RabbitAdmin rabbitAdmin;
  private final RunStats stats;

  private volatile ExecutorService pool;

  public ClientTopologyProvisioner(RagingClientsProperties props, RabbitAdmin rabbitAdmin, RunStats stats) {
    this.props = props;
    this.rabbitAdmin = rabbitAdmin;
    this.stats = stats;
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

    List<TopicExchange> topics =
        contexts.stream()
            .map(c -> new TopicExchange(ClientKeyspace.exchangeName(c, props.getTopicSuffix()), true, false))
            .toList();
    topics.forEach(rabbitAdmin::declareExchange);
    List<FanoutExchange> fanouts =
        fanoutNames.stream().map(n -> new FanoutExchange(n, true, false)).toList();
    fanouts.forEach(rabbitAdmin::declareExchange);

    long bindingsPerClient = (long) contexts.size() * topicKeys + fanouts.size();
    long totalObjects = (long) clients * (1 + bindingsPerClient);
    int width = ClientKeyspace.widthFor(clients);
    stats.beginProvision(clients, 1 + bindingsPerClient);
    log.info(
        "Declaring topology: {} clients x (1 queue + {} contexts x {} keys + {} fanout bindings) = {} objects"
            + " (topics {}, fanouts {}, {} threads). First queue: '{}'.",
        clients,
        contexts.size(),
        topicKeys,
        fanouts.size(),
        totalObjects,
        topics.stream().map(TopicExchange::getName).toList(),
        fanoutNames,
        concurrency,
        ClientKeyspace.queueName(props.getQueuePrefix(), 1, width));

    ThreadFactory daemonFactory =
        r -> {
          Thread t = new Thread(r, "provision-worker");
          t.setDaemon(true);
          return t;
        };
    ExecutorService executor = Executors.newFixedThreadPool(concurrency, daemonFactory);
    pool = executor;
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    long start = System.nanoTime();

    // Checkpoint per client so Ctrl-C / SIGTERM ends the run promptly instead of
    // waiting out the whole declaration backlog.
    for (int i = 1; i <= clients && failure.get() == null && !stats.isCancelled(); i++) {
      final int n = i;
      executor.submit(
          () -> {
            try {
              declareOne(n, width, contexts, topics, topicKeys, fanouts);
              stats.clientDone();
            } catch (RuntimeException e) {
              failure.compareAndSet(null, e);
            }
          });
    }
    executor.shutdown();
    try {
      while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        if (stats.isCancelled()) {
          executor.shutdownNow();
          // System.out: the logging system may already be torn down mid-shutdown.
          System.out.printf(
              "%nProvisioning cancelled: partial topology (%d of %d clients). Shutting down.%n",
              stats.clientsDone(), clients);
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
      log.warn("Provisioning interrupted: partial topology. Shutting down.");
      return;
    }
    if (failure.get() != null && !stats.isCancelled()) {
      throw new IllegalStateException("Topology declaration failed", failure.get());
    }
    if (stats.isCancelled()) {
      // System.out: the logging system may already be torn down mid-shutdown.
      System.out.printf(
          "%nProvisioning cancelled: partial topology (%d of %d clients). Shutting down.%n",
          stats.clientsDone(), clients);
      return;
    }
    stats.endProvision();

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
    // Classic queue: durable, non-exclusive, non-auto-delete. No args -> lightest possible.
    Queue queue = new Queue(ClientKeyspace.queueName(props.getQueuePrefix(), n, width), props.isDurable(), false, false);
    rabbitAdmin.declareQueue(queue);
    for (int c = 0; c < contexts.size(); c++) {
      for (int k = 0; k < topicKeys; k++) {
        rabbitAdmin.declareBinding(
            BindingBuilder.bind(queue)
                .to(topics.get(c))
                .with(ClientKeyspace.topicKey(props.getRoutingKeyPrefix(), n, width, contexts.get(c), k)));
      }
    }
    for (FanoutExchange fanout : fanouts) {
      rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(fanout));
    }
  }

  @Override
  public void destroy() {
    stats.cancel();
    ExecutorService executor = pool;
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
