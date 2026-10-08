package example.raging_rabbits;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

/**
 * Single source of truth for run progress, read by the {@link ConsoleMonitor} to render
 * the live status screen. Updated by the provisioner and the noise publisher.
 */
@Component
public class RunStats {

  enum Phase {
    STARTING,
    PROVISIONING,
    PROVISIONED,
    NOISE_RUNNING,
    DONE
  }

  private final long appStartNanos = System.nanoTime();

  private volatile Phase phase = Phase.STARTING;
  private volatile int totalClients;
  private volatile long objectsPerClient;
  private volatile long provisionStartNanos;
  private final AtomicInteger clientsDone = new AtomicInteger();

  private volatile double targetNoiseRate;
  private volatile long noiseStartNanos;
  private final AtomicLong noisePublished = new AtomicLong();

  private volatile int drainWorkers;
  private volatile int drainQueues;
  private volatile int drainConnections;
  private volatile int drainChannels;
  private volatile long drainStartNanos;
  private final AtomicLong consumed = new AtomicLong();

  void beginProvision(int totalClients, long objectsPerClient) {
    this.totalClients = totalClients;
    this.objectsPerClient = objectsPerClient;
    this.provisionStartNanos = System.nanoTime();
    this.phase = Phase.PROVISIONING;
  }

  void clientDone() {
    clientsDone.incrementAndGet();
  }

  void endProvision() {
    phase = Phase.PROVISIONED;
  }

  void beginNoise(double targetRate) {
    this.targetNoiseRate = targetRate;
    this.noiseStartNanos = System.nanoTime();
    this.phase = Phase.NOISE_RUNNING;
  }

  void noiseSilent() {
    this.phase = Phase.NOISE_RUNNING;
  }

  void beginDrain(int workers, int queues, int connections, int channels) {
    this.drainWorkers = workers;
    this.drainQueues = queues;
    this.drainConnections = connections;
    this.drainChannels = channels;
    this.drainStartNanos = System.nanoTime();
  }

  void consumed() {
    consumed.incrementAndGet();
  }

  void noisePublished() {
    noisePublished.incrementAndGet();
  }

  void done() {
    phase = Phase.DONE;
  }

  Phase phase() {
    return phase;
  }

  int clientsDone() {
    return clientsDone.get();
  }

  int totalClients() {
    return totalClients;
  }

  long objectsDeclared() {
    return (long) clientsDone.get() * objectsPerClient;
  }

  long totalObjects() {
    return (long) totalClients * objectsPerClient;
  }

  long noisePublishedCount() {
    return noisePublished.get();
  }

  boolean drainActive() {
    return drainStartNanos != 0;
  }

  int drainWorkers() {
    return drainWorkers;
  }

  int drainQueues() {
    return drainQueues;
  }

  int drainConnections() {
    return drainConnections;
  }

  int drainChannels() {
    return drainChannels;
  }

  long consumedCount() {
    return consumed.get();
  }

  double drainRate() {
    if (!drainActive()) {
      return 0;
    }
    double elapsed = (System.nanoTime() - drainStartNanos) / 1_000_000_000.0;
    return consumed.get() / Math.max(elapsed, 0.001);
  }

  double targetNoiseRate() {
    return targetNoiseRate;
  }

  double provisionObjectsPerSec() {
    double elapsed = (System.nanoTime() - provisionStartNanos) / 1_000_000_000.0;
    return objectsDeclared() / Math.max(elapsed, 0.001);
  }

  long provisionEtaSeconds() {
    double rate = provisionObjectsPerSec();
    if (rate <= 0) {
      return -1;
    }
    return (long) ((totalObjects() - objectsDeclared()) / rate);
  }

  double noiseActualRate() {
    if (phase != Phase.NOISE_RUNNING || noiseStartNanos == 0) {
      return 0;
    }
    double elapsed = (System.nanoTime() - noiseStartNanos) / 1_000_000_000.0;
    return noisePublished.get() / Math.max(elapsed, 0.001);
  }

  long elapsedSeconds() {
    return (System.nanoTime() - appStartNanos) / 1_000_000_000L;
  }

  static String formatDuration(long totalSeconds) {
    if (totalSeconds < 0) {
      return "--:--";
    }
    return "%02d:%02d".formatted(totalSeconds / 60, totalSeconds % 60);
  }
}
