package example.raging_rabbits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
 * Scalable drain-only consumers: one auto-ack consumer per client queue, multiplexed over a
 * small pool of shared channels. Messages are acknowledged on delivery with no handling and
 * no per-message logging — only a counter feeds the status screen. This keeps queue depths
 * (and broker memory) bounded while noise is publishing.
 *
 * <p>{@code app.consumers} / {@code CONSUMERS}: {@code all} (default, every client queue),
 * {@code off}, or a number (first N queues). Channels via {@code DRAIN_CHANNELS} (default 16).
 */
@Component
@Order(30)
public class NoiseConsumers implements ApplicationRunner, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(NoiseConsumers.class);

  private final RagingClientsProperties props;
  private final ConnectionFactory connectionFactory;
  private final RunStats stats;

  private Connection connection;
  private final List<Channel> channels = new ArrayList<>();
  private final List<ConsumerHandle> consumers = new ArrayList<>();

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
    int channelCount = Math.min(64, Math.max(1, props.getDrainChannels()));
    int width = ClientKeyspace.widthFor(props.getClients());

    connection = connectionFactory.createConnection();
    for (int i = 0; i < channelCount; i++) {
      channels.add(connection.createChannel(false));
    }
    for (int n = 1; n <= count; n++) {
      Channel channel = channels.get((n - 1) % channels.size());
      String queue = ClientKeyspace.queueName(props.getQueuePrefix(), n, width);
      String tag = channel.basicConsume(queue, true, (consumerTag, delivery) -> stats.consumed(), consumerTag -> {});
      consumers.add(new ConsumerHandle(channel, tag));
    }
    stats.beginDrain(count, channelCount);
    log.info("Drain on: {} consumers across {} channels (auto-ack, counting only).", count, channelCount);
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
    for (ConsumerHandle handle : consumers) {
      try {
        handle.channel().basicCancel(handle.tag());
      } catch (IOException e) {
        log.debug("Cancel failed: {}", e.getMessage());
      }
    }
    consumers.clear();
    for (Channel channel : channels) {
      try {
        channel.close();
      } catch (IOException | TimeoutException e) {
        log.debug("Channel close failed: {}", e.getMessage());
      }
    }
    channels.clear();
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  private record ConsumerHandle(Channel channel, String tag) {}
}
