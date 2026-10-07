package example.raging_rabbits;

import java.util.List;
import java.util.Map;

/**
 * Shared keyspace formulas for the baseline topology, used both when declaring bindings
 * ({@link ClientTopologyProvisioner}) and when publishing noise ({@link NoisePublisher})
 * so every published topic message matches a real binding.
 */
final class ClientKeyspace {

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

  private ClientKeyspace() {}

  static List<String> subjectsFor(String context) {
    return SUBJECTS.getOrDefault(context, GENERIC_SUBJECTS);
  }

  static int widthFor(int clients) {
    return Math.max(6, String.valueOf(clients).length());
  }

  static String pad(int i, int width) {
    String s = Integer.toString(i);
    return s.length() >= width ? s : "0".repeat(width - s.length()) + s;
  }

  static String queueName(String prefix, int n, int width) {
    return prefix + pad(n, width);
  }

  static String exchangeName(String context, String suffix) {
    return context + "." + suffix;
  }

  /** Exact binding/publish key for client {@code n}, slot {@code k}: {@code <prefix><id>.<subject>}. */
  static String topicKey(String routingPrefix, int n, int width, String context, int k) {
    List<String> subjects = subjectsFor(context);
    return routingPrefix + pad(n, width) + "." + subjects.get(Math.floorMod(n + k, subjects.size()));
  }
}
