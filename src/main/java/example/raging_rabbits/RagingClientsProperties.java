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
 * CLIENTS=20000 TOPIC_CONTEXTS=orders,payments,shipping TOPIC_KEYS=3 java -jar target/raging-rabbits-*.jar
 * </pre>
 */
@ConfigurationProperties(prefix = "app")
public class RagingClientsProperties {

  /** Number of client queues to create. Sourced from {@code CLIENTS}. */
  private int clients = 20000;

  /**
   * Bounded contexts, one topic exchange each ({@code <context>.<topic-exchange-suffix>}).
   * Default 6 of the 5-7 range; override with {@code TOPIC_CONTEXTS} (comma-separated).
   */
  private List<String> topicContexts =
      new ArrayList<>(List.of("orders", "payments", "shipping", "notifications", "billing", "inventory"));

  /** Suffix for per-context topic exchanges: {@code orders} -> {@code orders.events}. */
  private String topicExchangeSuffix = "events";

  /**
   * Routing keys bound per client per context exchange (1-3, clamped to max 3).
   * Keys look like {@code client.000001.order.created}. Default 2 (typical of the 1-3 range).
   */
  private int topicKeys = 2;

  /** Shared fanout exchanges; each client queue gets one binding to each of them. */
  private List<String> fanoutExchanges =
      new ArrayList<>(List.of("broadcast.announcements", "broadcast.alerts", "broadcast.config"));

  /** Queue name prefix. Final name is {@code <prefix><zero-padded id>}, e.g. {@code client.000001}. */
  private String queuePrefix = "client.";

  /** Routing key prefix. Topic keys are {@code <prefix><id>.<subject>}. Defaults to queuePrefix. */
  private String routingKeyPrefix;

  /** Declare queues as durable (survive broker restart). */
  private boolean durable = true;

  /** Parallel declaration threads. RabbitAdmin is thread-safe; each op uses its own channel. */
  private int concurrency = 8;

  /** Log progress every N clients. */
  private int logEvery = 1000;

  public int getClients() {
    return clients;
  }

  public void setClients(int clients) {
    this.clients = clients;
  }

  public List<String> getTopicContexts() {
    return topicContexts;
  }

  public void setTopicContexts(List<String> topicContexts) {
    this.topicContexts = topicContexts;
  }

  public String getTopicExchangeSuffix() {
    return topicExchangeSuffix;
  }

  public void setTopicExchangeSuffix(String topicExchangeSuffix) {
    this.topicExchangeSuffix = topicExchangeSuffix;
  }

  public int getTopicKeys() {
    return topicKeys;
  }

  public void setTopicKeys(int topicKeys) {
    this.topicKeys = topicKeys;
  }

  public List<String> getFanoutExchanges() {
    return fanoutExchanges;
  }

  public void setFanoutExchanges(List<String> fanoutExchanges) {
    this.fanoutExchanges = fanoutExchanges;
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

  public int getLogEvery() {
    return logEvery;
  }

  public void setLogEvery(int logEvery) {
    this.logEvery = logEvery;
  }
}
