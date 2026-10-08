package example.raging_rabbits;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Scalable drain-only consumers: {@code CONSUMERS} virtual-thread workers, each owning one
 * connection and draining a contiguous shard of the client queues (e.g. 1000 clients / 5 workers
 * = 200 queues each). One auto-ack consumer per queue, multiplexed over a few shared channels
 * per worker. Messages are acknowledged on delivery with no handling and no per-message
 * logging — only a counter feeds the status screen. This keeps queue depths (and broker memory)
 * bounded while noise is publishing, and loads the <em>broker</em> rather than the JVM.
 *
 * <p>Two scaling rules matter here, learned the hard way:
 *
 * <ul>
 *   <li>Workers attach <em>before</em> the noise publisher starts ({@code @Order(15)} vs
 *       {@code 20}), so registration RPCs complete on a silent broker. Attaching while a
 *       delivery flood is already flowing can starve the Consume-Ok replies on a shared
 *       connection and stall registration partway.
 *   <li>Each worker owns a <em>physical</em> connection (raw client factory, not Spring's
 *       single-connection cache), so delivery dispatch and control RPCs never share one
 *       connection pipeline across the fleet.
 * </ul>
 *
 * <p>{@code app.consumers} / {@code CONSUMERS}: a worker count (default 8), or {@code off}.
 */
@Component
@Order(15)
public class NoiseConsumers implements ApplicationRunner, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(NoiseConsumers.class);

  private final RagingClientsProperties props;
  private final RabbitProperties rabbitProps;
  private final RunStats stats;

  private final List<Connection> connections = new ArrayList<>();
  private final List<Channel> channels = new ArrayList<>();

  public NoiseConsumers(
      RagingClientsProperties props, RabbitProperties rabbitProps, RunStats stats) {
    this.props = props;
    this.rabbitProps = rabbitProps;
    this.stats = stats;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    int workers = resolveWorkers();
    if (workers <= 0) {
      log.info("Drain workers off (app.consumers='{}').", props.getConsumers());
      return;
    }
    int clients = props.getClients();
    int channelsPerWorker = Math.max(1, props.getDrainChannels());
    int width = ClientKeyspace.widthFor(clients);

    ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    try {
      List<Future<ShardResources>> futures = new ArrayList<>();
      for (int w = 0; w < workers; w++) {
        int from = 1 + w * clients / workers;
        int to = 1 + (w + 1) * clients / workers;
        final int worker = w;
        futures.add(pool.submit(() -> attachShard(worker, from, to, width, channelsPerWorker)));
      }
      for (Future<ShardResources> future : futures) {
        try {
          ShardResources resources = future.get();
          connections.add(resources.connection());
          channels.addAll(resources.channels());
        } catch (ExecutionException e) {
          throw new IllegalStateException("Drain attach failed", e.getCause());
        }
      }
    } finally {
      pool.shutdown();
    }
    stats.beginDrain(workers, clients, connections.size(), channels.size());
    log.info(
        "Drain on: {} workers x ~{} queues across {} connections / {} channels (auto-ack, counting only).",
        workers,
        clients / workers,
        connections.size(),
        channels.size());
  }

  private ShardResources attachShard(int worker, int from, int to, int width, int channelsPerWorker) throws Exception {
    // NOTE: raw client factory, not Spring's CachingConnectionFactory — the latter hands out
    // proxies to a single shared physical connection, which would put every worker back on
    // one pipeline (the exact starvation this design avoids). Single-host is enough here;
    // multi-address clusters can extend this with getAddresses().
    com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
    factory.setHost(rabbitProps.getHost());
    factory.setPort(rabbitProps.getPort());
    factory.setUsername(rabbitProps.getUsername());
    factory.setPassword(rabbitProps.getPassword());
    String vhost = rabbitProps.getVirtualHost();
    factory.setVirtualHost(vhost == null || vhost.isBlank() ? "/" : vhost);
    Connection connection = factory.newConnection("drain-w" + worker);
    List<Channel> shardChannels = new ArrayList<>();
    for (int c = 0; c < channelsPerWorker; c++) {
      shardChannels.add(connection.createChannel());
    }
    for (int n = from; n < to; n++) {
      Channel channel = shardChannels.get((n - from) % shardChannels.size());
      String queue = ClientKeyspace.queueName(props.getQueuePrefix(), n, width);
      channel.basicConsume(queue, true, (consumerTag, delivery) -> stats.consumed(), consumerTag -> {});
    }
    return new ShardResources(connection, shardChannels);
  }

  private int resolveWorkers() {
    String spec = props.getConsumers() == null ? "" : props.getConsumers().trim().toLowerCase();
    if (spec.isBlank() || spec.equals("off")) {
      return 0;
    }
    try {
      int workers = Integer.parseInt(spec);
      if (workers <= 0) {
        return 0;
      }
      return Math.min(workers, Math.max(1, props.getClients()));
    } catch (NumberFormatException e) {
      log.warn("Unknown app.consumers='{}', drain staying off (expected off|<worker count>).", props.getConsumers());
      return 0;
    }
  }

  @Override
  public void destroy() {
    // Closing a connection drops all of its consumers server-side at once: no per-consumer
    // cancel RPCs (which could stall the same way registration did under load).
    for (Channel channel : channels) {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (Exception e) {
        log.debug("Channel close failed: {}", e.getMessage());
      }
    }
    channels.clear();
    for (Connection connection : connections) {
      try {
        connection.close();
      } catch (Exception e) {
        log.debug("Connection close failed: {}", e.getMessage());
      }
    }
    connections.clear();
  }

  private record ShardResources(Connection connection, List<Channel> channels) {}
}
