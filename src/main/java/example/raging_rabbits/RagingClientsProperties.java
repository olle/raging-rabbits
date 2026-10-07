package example.raging_rabbits;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunables for the scale test.
 *
 * <p>Baseline scenario: {@code CLIENTS} individual clients (default 20_000), each with one
 * queue, {@code TOPIC_BINDINGS} bindings to the shared topic exchange (default 7, the max of
 * the 5-7 range) and one binding per shared fanout exchange (default 3 exchanges, the max of
 * the 2-3 range).
 *
 * <pre>
 * CLIENTS=20000 ./mvnw spring-boot:run
 * CLIENTS=20000 TOPIC_BINDINGS=5 FANOUT_EXCHANGES=broadcast.a,broadcast.b java -jar target/raging-rabbits-*.jar
 * </pre>
 */
@ConfigurationProperties(prefix = "app")
public class RagingClientsProperties {

  /** Number of client queues to create. Sourced from {@code CLIENTS}. */
  private int clients = 20000;

  /** The shared topic exchange every queue binds to (multiple keys per queue). */
  private String exchange = "clients.topic";

  /** Topic bindings per client queue. Keys are {@code <routing-key-prefix><id>.s1..sN}. */
  private int topicBindings = 7;

  /** Shared fanout exchanges; each client queue gets one binding to each of them. */
  private List<String> fanoutExchanges = new ArrayList<>(List.of("broadcast.alpha", "broadcast.beta", "broadcast.gamma"));

  /** Parallel declaration threads. RabbitAdmin is thread-safe; each op uses its own channel. */
  private int concurrency = 8;

  /** Queue name prefix. Final name is {@code <prefix><zero-padded id>}, e.g. {@code client.000001}. */
  private String queuePrefix = "client.";

  /** Routing key prefix. Final key is {@code <prefix><zero-padded id>}. Defaults to queuePrefix. */
  private String routingKeyPrefix;

  /** Declare queues as durable (survive broker restart). */
  private boolean durable = true;

  /** Log progress every N clients. */
  private int logEvery = 1000;

  public int getClients() {
    return clients;
  }

  public void setClients(int clients) {
    this.clients = clients;
  }

  public String getExchange() {
    return exchange;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public int getTopicBindings() {
    return topicBindings;
  }

  public void setTopicBindings(int topicBindings) {
    this.topicBindings = topicBindings;
  }

  public List<String> getFanoutExchanges() {
    return fanoutExchanges;
  }

  public void setFanoutExchanges(List<String> fanoutExchanges) {
    this.fanoutExchanges = fanoutExchanges;
  }

  public int getConcurrency() {
    return concurrency;
  }

  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
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

  public int getLogEvery() {
    return logEvery;
  }

  public void setLogEvery(int logEvery) {
    this.logEvery = logEvery;
  }
}
