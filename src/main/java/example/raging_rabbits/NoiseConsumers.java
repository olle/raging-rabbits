package example.raging_rabbits;

import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Scalable drain-only consumers: one auto-ack consumer per client queue, spread over several
 * connections with a small pool of shared channels each. Messages are acknowledged on delivery
 * with no handling and no per-message logging — only a counter feeds the status screen. This
 * keeps queue depths (and broker memory) bounded while noise is publishing.
 *
 * <p>Two scaling rules matter here, learned the hard way:
 *
 * <ul>
 *   <li>Consumers attach <em>before</em> the noise publisher starts ({@code @Order(15)} vs
 *       {@code 20}), so registration RPCs complete on a silent broker. Attaching while a
 *       delivery flood is already flowing can starve the Consume-Ok replies on a shared
 *       connection and stall registration partway.
 *   <li>Consumers spread over {@code DRAIN_CONNECTIONS} connections (default 4) instead of one,
 *       so delivery dispatch and control RPCs don't share a single connection pipeline.
 * </ul>
 *
 * <p>{@code app.consumers} / {@code CONSUMERS}: {@code all} (default, every client queue),
 * {@code off}, or a number (first N queues).
 */
@Component
@Order(15)
public class NoiseConsumers implements ApplicationRunner, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(NoiseConsumers.class);

  private final RagingClientsProperties props;
  private final ConnectionFactory connectionFactory;
  private final RunStats stats;

  private final List<Connection> connections = new ArrayList<>();
  private final List<Channel> channels = new ArrayList<>();

  public NoiseConsumers(
      RagingClientsProperties props, ConnectionFactory connectionFactory, RunStats stats) {
    this.props = props;
    this.connectionFactory = connectionFactory;
    this.stats = stats;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    int count = resolveCount();
    if (count <= 0) {
      log.info("Drain consumers off (app.consumers='{}').", props.getConsumers());
      return;
    }
    int connectionCount = Math.min(Math.max(1, props.getDrainConnections()), count);
    int totalChannels = Math.max(connectionCount, props.getDrainChannels());
    int width = ClientKeyspace.widthFor(props.getClients());

    for (int i = 0; i < connectionCount; i++) {
      Connection connection = connectionFactory.createConnection();
      connections.add(connection);
      int channelsForThis = totalChannels / connectionCount + (i < totalChannels % connectionCount ? 1 : 0);
      for (int c = 0; c < channelsForThis; c++) {
        channels.add(connection.createChannel(false));
      }
    }
    for (int n = 1; n <= count; n++) {
      Channel channel = channels.get((n - 1) % channels.size());
      String queue = ClientKeyspace.queueName(props.getQueuePrefix(), n, width);
      channel.basicConsume(queue, true, (consumerTag, delivery) -> stats.consumed(), consumerTag -> {});
    }
    stats.beginDrain(count, connectionCount, channels.size());
    log.info(
        "Drain on: {} consumers across {} connections / {} channels (auto-ack, counting only).",
        count,
        connectionCount,
        channels.size());
  }

  private int resolveCount() {
    String spec = props.getConsumers() == null ? "all" : props.getConsumers().trim().toLowerCase();
    return switch (spec) {
      case "off", "" -> 0;
      case "all" -> Math.max(0, props.getClients());
      default -> {
        try {
          yield Math.min(Math.max(0, Integer.parseInt(spec)), Math.max(0, props.getClients()));
        } catch (NumberFormatException e) {
          log.warn("Unknown app.consumers='{}', drain staying off (expected off|all|<n>).", props.getConsumers());
          yield 0;
        }
      }
    };
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
}
