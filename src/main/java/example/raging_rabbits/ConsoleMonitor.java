package example.raging_rabbits;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Live statistical overview of the run: phases ({@code STARTING → PROVISIONING → NOISE}),
 * client/topology progress with rate + ETA, and noise throughput.
 *
 * <p>On a terminal ({@code app.monitor=auto} + TTY, or forced with {@code console}) it redraws
 * one status block in place at 1 Hz instead of scrolling log lines. Piped/redirected output
 * (or {@code log} mode) falls back to a single summary log line every few seconds.
 */
@Component
@Order(5)
public class ConsoleMonitor implements ApplicationRunner, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(ConsoleMonitor.class);
  private static final int BAR_WIDTH = 20;

  private final RunStats stats;
  private final RagingClientsProperties props;

  private ScheduledExecutorService scheduler;
  private boolean tty;
  private int blockLines;
  private final AtomicLong ticks = new AtomicLong();

  public ConsoleMonitor(RunStats stats, RagingClientsProperties props) {
    this.stats = stats;
    this.props = props;
  }

  @Override
  public void run(ApplicationArguments args) {
    String mode = props.getMonitor() == null ? "auto" : props.getMonitor().trim().toLowerCase();
    tty = switch (mode) {
      case "console" -> true;
      case "log" -> false;
      default -> System.console() != null;
    };
    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "console-monitor");
      t.setDaemon(true);
      return t;
    });
    if (tty) {
      synchronized (System.out) {
        System.out.println("raging-rabbits · baseline status");
        blockLines = render().size();
        render().forEach(System.out::println);
      }
    } else {
      log.info("Status monitor in log mode (1 summary line every 5s).");
    }
    scheduler.scheduleAtFixedRate(this::tick, 1, 1, TimeUnit.SECONDS);
  }

  private void tick() {
    long tick = ticks.incrementAndGet();
    try {
      if (tty) {
        List<String> lines = render();
        synchronized (System.out) {
          System.out.print("\033[" + lines.size() + "A");
          lines.forEach(l -> System.out.print(l + "\033[K\n"));
          System.out.flush();
        }
      } else if (tick % 5 == 0) {
        log.info("status {}", oneLiner());
      }
    } catch (RuntimeException e) {
      log.debug("Status tick failed: {}", e.getMessage());
    }
  }

  private List<String> render() {
    List<String> lines = new ArrayList<>();
    int done = stats.clientsDone();
    int total = Math.max(1, stats.totalClients());
    double pct = 100.0 * done / total;
    int filled = (int) Math.round(BAR_WIDTH * done / (double) total);
    String bar = "█".repeat(Math.max(0, filled)) + "─".repeat(Math.max(0, BAR_WIDTH - filled));

    lines.add("phase    " + phaseLabel());
    lines.add(
        "clients  [%s] %,d / %,d (%d%%) · %,.0f obj/s · ETA %s"
            .formatted(
                bar,
                done,
                stats.totalClients(),
                (int) pct,
                stats.provisionObjectsPerSec(),
                RunStats.formatDuration(stats.provisionEtaSeconds())));
    lines.add("objects  %,d / %,d declared".formatted(stats.objectsDeclared(), stats.totalObjects()));
    lines.add("noise    " + noiseLabel());
    lines.add("drain    " + drainLabel());
    lines.add("elapsed  " + RunStats.formatDuration(stats.elapsedSeconds()));
    return lines;
  }

  private String phaseLabel() {
    return switch (stats.phase()) {
      case STARTING -> "STARTING";
      case PROVISIONING -> "PROVISIONING topology";
      case PROVISIONED -> "provisioned · starting noise";
      case NOISE_RUNNING -> stats.targetNoiseRate() > 0 ? "NOISE running" : "idling (noise off)";
      case DONE -> "DONE";
    };
  }

  private String noiseLabel() {
    if (stats.phase().ordinal() < RunStats.Phase.NOISE_RUNNING.ordinal()) {
      return "waiting for provisioning…";
    }
    if (stats.targetNoiseRate() <= 0) {
      return "off";
    }
    return "on · target %,.1f/s · actual %,.1f/s · %,d published"
        .formatted(stats.targetNoiseRate(), stats.noiseActualRate(), stats.noisePublishedCount());
  }

  private String drainLabel() {
    if (!stats.drainActive()) {
      return "off";
    }
    return "on · %,d workers · %,d queues · %d conns · %,d consumed (%,.1f/s)"
        .formatted(stats.drainWorkers(), stats.drainQueues(), stats.drainConnections(), stats.consumedCount(), stats.drainRate());
  }

  private String oneLiner() {
    return "phase=%s clients=%d/%d objs=%,d (%,.0f/s, ETA %s) noise=%,d pub (%,.1f/s) drain=%,d (%,.1f/s) elapsed=%s"
        .formatted(
            stats.phase(),
            stats.clientsDone(),
            stats.totalClients(),
            stats.objectsDeclared(),
            stats.provisionObjectsPerSec(),
            RunStats.formatDuration(stats.provisionEtaSeconds()),
            stats.noisePublishedCount(),
            stats.noiseActualRate(),
            stats.consumedCount(),
            stats.drainRate(),
            RunStats.formatDuration(stats.elapsedSeconds()));
  }

  @Override
  public void destroy() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    stats.done();
    if (tty) {
      List<String> lines = render();
      synchronized (System.out) {
        System.out.print("\033[" + lines.size() + "A");
        lines.forEach(l -> System.out.print(l + "\033[K\n"));
        System.out.println("run complete · %,d clients · %,d objects · %,d noise msgs · %,d consumed · %s elapsed"
            .formatted(
                stats.clientsDone(),
                stats.objectsDeclared(),
                stats.noisePublishedCount(),
                stats.consumedCount(),
                RunStats.formatDuration(stats.elapsedSeconds())));
        System.out.flush();
      }
    } else {
      log.info(
          "run complete · {} clients · {} objects · {} noise msgs · {} consumed · {} elapsed",
          String.format("%,d", stats.clientsDone()),
          String.format("%,d", stats.objectsDeclared()),
          String.format("%,d", stats.noisePublishedCount()),
          String.format("%,d", stats.consumedCount()),
          RunStats.formatDuration(stats.elapsedSeconds()));
    }
  }
}
