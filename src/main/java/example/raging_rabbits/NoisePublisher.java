package example.raging_rabbits;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Publishes a little messaging noise after provisioning: steady, paced messages round-robin
 * across every topic exchange and fanout exchange. Topic messages reuse the exact binding-key
 * formula ({@link ClientKeyspace}) for a random client, so each one routes into a real queue.
 *
 * <p>Levels ({@code app.noise} / {@code NOISE}): {@code off} (default), {@code low} (5 msg/s),
 * {@code medium} (25 msg/s), {@code high} (100 msg/s); a plain number sets a custom msg/s rate.
 * Every message carries a short per-message TTL ({@code app.noise-ttl-ms}) so noise evaporates
 * instead of filling tens of thousands of consumerless queues without bound.
 */
@Component
@Order(20)
public class NoisePublisher implements ApplicationRunner, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(NoisePublisher.class);

  private final RagingClientsProperties props;
  private final RabbitTemplate rabbitTemplate;
  private final RunStats stats;

  private volatile boolean running;
  private Thread thread;

  public NoisePublisher(RagingClientsProperties props, RabbitTemplate rabbitTemplate, RunStats stats) {
    this.props = props;
    this.rabbitTemplate = rabbitTemplate;
    this.stats = stats;
  }

  @Override
  public void run(ApplicationArguments args) {
    // A cancelled predecessor must not start new work.
    if (stats.isCancelled()) {
      log.info("Skipping noise: run was cancelled.");
      return;
    }
    double rate = rateFor(props.getNoise());
    if (rate <= 0) {
      log.info("Noise off (app.noise='{}'). Topology stays silent.", props.getNoise());
      stats.noiseSilent();
      return;
    }
    List<String> contexts =
        props.getTopics().stream().filter(c -> c != null && !c.isBlank()).toList();
    List<String> fanouts =
        props.getFanouts().stream().filter(n -> n != null && !n.isBlank()).toList();
    if (contexts.isEmpty() && fanouts.isEmpty()) {
      log.warn("Noise requested but no topic contexts or fanout exchanges configured; staying silent.");
      return;
    }
    List<Destination> plan = new ArrayList<>();
    contexts.forEach(c -> plan.add(new Destination.Topic(c)));
    fanouts.forEach(f -> plan.add(new Destination.Fanout(f)));
    log.info(
        "Noise on: {} msg/s across {} destinations ({} topics, {} fanouts), per-message TTL {}ms.",
        rate,
        plan.size(),
        contexts.size(),
        fanouts.size(),
        props.getNoiseTtlMs());

    running = true;
    stats.beginNoise(rate);
    thread = new Thread(() -> loop(rate, plan), "noise-publisher");
    thread.setDaemon(true);
    thread.start();
  }

  private void loop(double rate, List<Destination> plan) {
    long intervalNanos = (long) (1_000_000_000.0 / rate);
    long max = Math.max(0, props.getNoiseMax());
    long seq = 0;
    while (running && (max <= 0 || seq < max)) {
      long start = System.nanoTime();
      try {
        publishOne(seq, plan.get((int) (seq % plan.size())));
        seq++;
        stats.noisePublished();
      } catch (AmqpException e) {
        log.warn("Noise publish failed (seq {}): {}", seq, e.getMessage());
      }
      long elapsed = System.nanoTime() - start;
      if (elapsed < intervalNanos) {
        LockSupport.parkNanos(intervalNanos - elapsed);
      }
      if (Thread.currentThread().isInterrupted()) {
        break;
      }
    }
    if (max > 0 && seq >= max) {
      log.info("Noise budget exhausted: {} messages published.", seq);
    } else {
      log.info("Noise stopped after {} messages.", seq);
    }
  }

  private void publishOne(long seq, Destination dest) {
    String exchange;
    String routingKey;
    String payload;
    if (dest instanceof Destination.Topic t) {
      int n = ThreadLocalRandom.current().nextInt(1, props.getClients() + 1);
      int k = ThreadLocalRandom.current().nextInt(Math.max(1, Math.min(3, props.getKeys())));
      int width = ClientKeyspace.widthFor(props.getClients());
      routingKey = ClientKeyspace.topicKey(props.getRoutingKeyPrefix(), n, width, t.context(), k);
      exchange = ClientKeyspace.exchangeName(t.context(), props.getTopicSuffix());
      payload =
          "{\"seq\":%d,\"client\":%d,\"context\":\"%s\",\"ts\":%d}".formatted(seq, n, t.context(), System.currentTimeMillis());
    } else {
      Destination.Fanout f = (Destination.Fanout) dest;
      exchange = f.name();
      routingKey = "";
      payload = "{\"seq\":%d,\"ts\":%d}".formatted(seq, System.currentTimeMillis());
    }
    MessageProperties messageProperties = new MessageProperties();
    messageProperties.setContentType("application/json");
    messageProperties.setMessageId("noise-" + seq);
    messageProperties.setExpiration(String.valueOf(Math.max(1000, props.getNoiseTtlMs())));
    rabbitTemplate.send(exchange, routingKey, new Message(payload.getBytes(StandardCharsets.UTF_8), messageProperties));
  }

  static double rateFor(String noise) {
    if (noise == null || noise.isBlank() || noise.equalsIgnoreCase("off")) {
      return 0;
    }
    switch (noise.trim().toLowerCase()) {
      case "low" -> {
        return 5;
      }
      case "medium" -> {
        return 25;
      }
      case "high" -> {
        return 100;
      }
      default -> {
        try {
          double custom = Double.parseDouble(noise.trim());
          if (custom > 0) {
            return custom;
          }
        } catch (NumberFormatException ignored) {
          // fall through to warning
        }
        log.warn("Unknown app.noise='{}', staying silent (expected off|low|medium|high or msg/s).", noise);
        return 0;
      }
    }
  }

  @Override
  public void destroy() {
    stats.cancel();
    running = false;
    if (thread != null) {
      thread.interrupt();
      try {
        thread.join(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private sealed interface Destination permits Destination.Topic, Destination.Fanout {
    record Topic(String context) implements Destination {}

    record Fanout(String name) implements Destination {}
  }
}
