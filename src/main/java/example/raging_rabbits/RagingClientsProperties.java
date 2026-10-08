package example.raging_rabbits;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunables for the scale test.
 *
 * <p>Topology model: one topic exchange per bounded context (DDD), each client binding
 * 1-3 routing keys per context exchange, plus one binding per shared fanout exchange
 * (broadcasts every client receives).
 *
 * <pre>
 * CLIENTS=20000 ./mvnw spring-boot:run
 * CLIENTS=20000 TOPICS=orders,payments,shipping KEYS=3 java -jar target/raging-rabbits-*.jar
 * </pre>
 */
@ConfigurationProperties(prefix = "app")
public class RagingClientsProperties {

  /** Number of client queues to create. Sourced from {@code CLIENTS}. */
  private int clients = 20000;

  /**
   * Bounded contexts, one topic exchange each ({@code <context>.<topic-suffix>}).
   * Default 6 of the 5-7 range; override with {@code TOPICS} (comma-separated).
   */
  private List<String> topics =
      new ArrayList<>(List.of("orders", "payments", "shipping", "notifications", "billing", "inventory"));

  /** Suffix for per-context topic exchanges: {@code orders} -> {@code orders.events}. */
  private String topicSuffix = "events";

  /**
   * Routing keys bound per client per context exchange (1-3, clamped to max 3).
   * Keys look like {@code client.000001.order.created}. Default 2 (typical of the 1-3 range).
   */
  private int keys = 2;

  /** Shared fanout exchanges; each client queue gets one binding to each of them. */
  private List<String> fanouts =
      new ArrayList<>(List.of("broadcast.announcements", "broadcast.alerts", "broadcast.config"));

  /** Queue name prefix. Final name is {@code <prefix><zero-padded id>}, e.g. {@code client.000001}. */
  private String queuePrefix = "client.";

  /** Routing key prefix. Topic keys are {@code <prefix><id>.<subject>}. Defaults to queuePrefix. */
  private String routingKeyPrefix;

  /** Declare queues as durable (survive broker restart). */
  private boolean durable = true;

  /** Parallel declaration threads. RabbitAdmin is thread-safe; each op uses its own channel. */
  private int concurrency = 8;

  /**
   * Messaging noise level: {@code off} (default), {@code low} (5 msg/s), {@code medium}
   * (25 msg/s), {@code high} (100 msg/s), or a plain number for a custom msg/s rate.
   */
  private String noise = "off";

  /** Per-message TTL (ms) for noise so it evaporates instead of filling queues without bound. */
  private long noiseTtlMs = 30000;

  /**
   * Status monitor rendering: {@code auto} (TTY-aware, default), {@code console} (force ANSI
   * screen), or {@code log} (periodic summary lines).
   */
  private String monitor = "auto";

  /**
   * Drain workers (virtual threads): each owns one connection and drains a shard of the
   * client queues, e.g. 1000 clients / 5 workers = 200 queues each. {@code off} disables.
   */
  private String consumers = "8";

  /** Channels per drain worker connection. */
  private int drainChannels = 4;

  public int getClients() {
    return clients;
  }

  public void setClients(int clients) {
    this.clients = clients;
  }

  public List<String> getTopics() {
    return topics;
  }

  public void setTopics(List<String> topics) {
    this.topics = topics;
  }

  public String getTopicSuffix() {
    return topicSuffix;
  }

  public void setTopicSuffix(String topicSuffix) {
    this.topicSuffix = topicSuffix;
  }

  public int getKeys() {
    return keys;
  }

  public void setKeys(int keys) {
    this.keys = keys;
  }

  public List<String> getFanouts() {
    return fanouts;
  }

  public void setFanouts(List<String> fanouts) {
    this.fanouts = fanouts;
  }

  public String getQueuePrefix() {
    return queuePrefix;
  }

  public void setQueuePrefix(String queuePrefix) {
    this.queuePrefix = queuePrefix;
  }

  public String getRoutingKeyPrefix() {
    return (routingKeyPrefix == null || routingKeyPrefix.isBlank()) ? queuePrefix : routingKeyPrefix;
  }

  public void setRoutingKeyPrefix(String routingKeyPrefix) {
    this.routingKeyPrefix = routingKeyPrefix;
  }

  public boolean isDurable() {
    return durable;
  }

  public void setDurable(boolean durable) {
    this.durable = durable;
  }

  public int getConcurrency() {
    return concurrency;
  }

  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
  }

  public String getNoise() {
    return noise;
  }

  public void setNoise(String noise) {
    this.noise = noise;
  }

  public long getNoiseTtlMs() {
    return noiseTtlMs;
  }

  public void setNoiseTtlMs(long noiseTtlMs) {
    this.noiseTtlMs = noiseTtlMs;
  }

  public String getMonitor() {
    return monitor;
  }

  public void setMonitor(String monitor) {
    this.monitor = monitor;
  }

  public String getConsumers() {
    return consumers;
  }

  public void setConsumers(String consumers) {
    this.consumers = consumers;
  }

  public int getDrainChannels() {
    return drainChannels;
  }

  public void setDrainChannels(int drainChannels) {
    this.drainChannels = drainChannels;
  }
}
