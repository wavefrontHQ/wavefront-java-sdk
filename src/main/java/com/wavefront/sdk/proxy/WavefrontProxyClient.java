package com.wavefront.sdk.proxy;

import com.wavefront.sdk.common.BufferFlusher;
import com.wavefront.sdk.common.NamedThreadFactory;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.entities.histograms.HistogramGranularity;
import com.wavefront.sdk.entities.histograms.WavefrontHistogramSender;
import com.wavefront.sdk.entities.metrics.WavefrontMetricSender;
import com.wavefront.sdk.entities.tracing.SpanLog;
import com.wavefront.sdk.entities.tracing.WavefrontTracingSpanSender;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.net.SocketFactory;

import static com.wavefront.sdk.common.Utils.histogramToLineData;
import static com.wavefront.sdk.common.Utils.metricToLineData;
import static com.wavefront.sdk.common.Utils.tracingSpanToLineData;

/**
 * WavefrontProxyClient that sends data directly via TCP to the Wavefront Proxy Agent.
 * User should probably attempt to reconnect when exceptions are thrown from any methods.
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class WavefrontProxyClient implements WavefrontMetricSender, WavefrontHistogramSender,
    WavefrontTracingSpanSender, BufferFlusher, Runnable, Closeable {

  private static final Logger LOGGER = Logger.getLogger(
      WavefrontProxyClient.class.getCanonicalName());

  @Nullable
  private final ProxyConnectionHandler metricsProxyConnectionHandler;

  @Nullable
  private final ProxyConnectionHandler histogramProxyConnectionHandler;

  @Nullable
  private final ProxyConnectionHandler tracingProxyConnectionHandler;

  /**
   * Source to use if entity source is null
   */
  private final String defaultSource = InetAddress.getLocalHost().getHostName();

  private final ScheduledExecutorService scheduler;

  public static class Builder {
    // Required parameters
    private final String proxyHostName;

    // Optional parameters
    private Integer metricsPort;
    private Integer distributionPort;
    private Integer tracingPort;
    private SocketFactory socketFactory = SocketFactory.getDefault();
    private int flushIntervalSeconds = 5;

    /**
     * WavefrontProxyClient.Builder
     *
     * @param proxyHostName     Hostname of the Wavefront proxy
     */
    public Builder(String proxyHostName) {
      this.proxyHostName = proxyHostName;
    }

    /**
     * Invoke this method to enable sending metrics to Wavefront cluster via proxy
     *
     * @param metricsPort       Metrics Port on which the Wavefront proxy is listening on
     * @return {@code this}
     */
    public Builder metricsPort(int metricsPort) {
      this.metricsPort = metricsPort;
      return this;
    }

    /**
     * Invoke this method to enable sending distribution to Wavefront cluster via proxy
     *
     * @param distributionPort   Distribution Port on which the Wavefront proxy is listening on
     * @return {@code this}
     */
    public Builder distributionPort(int distributionPort) {
      this.distributionPort = distributionPort;
      return this;
    }

    /**
     * Invoke this method to enable sending tracing spans to Wavefront cluster via proxy
     *
     * @param tracingPort        Tracing Port on which the Wavefront proxy is listening on
     * @return {@code this}
     */
    public Builder tracingPort(int tracingPort) {
      this.tracingPort = tracingPort;
      return this;
    }

    /**
     * Set an explicit SocketFactory
     *
     * @param socketFactory       SocketFactory
     * @return {@code this}
     */
    public Builder socketFactory(SocketFactory socketFactory) {
      this.socketFactory = socketFactory;
      return this;
    }

    /**
     * Set interval at which you want to flush points to Wavefront proxy
     *
     * @param flushIntervalSeconds  Interval at which you want to flush points to Wavefront proxy
     * @return {@code this}
     */
    public Builder flushIntervalSeconds(int flushIntervalSeconds) {
      this.flushIntervalSeconds = flushIntervalSeconds;
      return this;
    }

    /**
     * Builds WavefrontProxyClient instance
     *
     * @return {@link WavefrontProxyClient}
     * @throws UnknownHostException
     */
    public WavefrontProxyClient build() throws UnknownHostException {
      return new WavefrontProxyClient(this);
    }
  }

  private WavefrontProxyClient(Builder builder) throws UnknownHostException {
    if (builder.metricsPort == null) {
      metricsProxyConnectionHandler = null;
    } else {
      metricsProxyConnectionHandler = new ProxyConnectionHandler(
          new InetSocketAddress(builder.proxyHostName, builder.metricsPort),
          builder.socketFactory);
    }

    if (builder.distributionPort == null) {
      histogramProxyConnectionHandler = null;
    } else {
      histogramProxyConnectionHandler = new ProxyConnectionHandler(
          new InetSocketAddress(builder.proxyHostName, builder.distributionPort),
          builder.socketFactory);
    }

    if (builder.tracingPort == null) {
      tracingProxyConnectionHandler = null;
    } else {
      tracingProxyConnectionHandler = new ProxyConnectionHandler(
          new InetSocketAddress(builder.proxyHostName, builder.tracingPort),
          builder.socketFactory);
    }

    scheduler = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("wavefrontProxySender"));
    // flush every 5 seconds
    scheduler.scheduleAtFixedRate(this, 1, builder.flushIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void sendMetric(String name, double value, @Nullable Long timestamp,
                         @Nullable String source, @Nullable Map<String, String> tags)
      throws IOException {
    if (!metricsProxyConnectionHandler.isConnected()) {
      try {
        metricsProxyConnectionHandler.connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }

    try {
      try {
        String lineData = metricToLineData(name, value, timestamp, source, tags, defaultSource);
        metricsProxyConnectionHandler.sendData(lineData);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      metricsProxyConnectionHandler.incrementFailureCount();
      throw e;
    }
  }

  @Override
  public void sendDistribution(String name, List<Pair<Double, Integer>> centroids,
                               Set<HistogramGranularity> histogramGranularities,
                               @Nullable Long timestamp, @Nullable String source,
                               @Nullable Map<String, String> tags)
      throws IOException {
    if (!histogramProxyConnectionHandler.isConnected()) {
      try {
        histogramProxyConnectionHandler.connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }

    try {
      String lineData = histogramToLineData(name, centroids, histogramGranularities, timestamp,
          source, tags, defaultSource);
      try {
        histogramProxyConnectionHandler.sendData(lineData);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      histogramProxyConnectionHandler.incrementFailureCount();
      throw e;
    }
  }

  @Override
  public void sendSpan(String name, long startMillis, long durationMillis,
                       @Nullable String source, UUID traceId, UUID spanId,
                       @Nullable List<UUID> parents, @Nullable List<UUID> followsFrom,
                       @Nullable List<Pair<String, String>> tags, @Nullable List<SpanLog> spanLogs)
      throws IOException {
    if (!tracingProxyConnectionHandler.isConnected()) {
      try {
        tracingProxyConnectionHandler.connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }

    try {
      String lineData = tracingSpanToLineData(name, startMillis, durationMillis, source, traceId,
          spanId, parents, followsFrom, tags, spanLogs, defaultSource);
      try {
        tracingProxyConnectionHandler.sendData(lineData);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      tracingProxyConnectionHandler.incrementFailureCount();
      throw e;
    }
  }

  @Override
  public void run() {
    try {
      this.flush();
    } catch (Throwable ex) {
      LOGGER.log(Level.FINE, "Unable to report to Wavefront cluster", ex);
    }
  }

  @Override
  public void flush() throws IOException {
    if (metricsProxyConnectionHandler != null) {
      metricsProxyConnectionHandler.flush();
    }

    if (histogramProxyConnectionHandler != null) {
      histogramProxyConnectionHandler.flush();
    }

    if (tracingProxyConnectionHandler != null) {
      tracingProxyConnectionHandler.flush();
    }
  }

  @Override
  public int getFailureCount() {
    int failureCount = 0;
    if (metricsProxyConnectionHandler != null) {
      failureCount += metricsProxyConnectionHandler.getFailureCount();
    }

    if (histogramProxyConnectionHandler != null) {
      failureCount += histogramProxyConnectionHandler.getFailureCount();
    }

    if (tracingProxyConnectionHandler != null) {
      failureCount += tracingProxyConnectionHandler.getFailureCount();
    }
    return failureCount;
  }

  @Override
  public void close() {
    // Flush before closing
    try {
      flush();
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "error flushing buffer", e);
    }

    try {
      scheduler.shutdownNow();
    } catch (SecurityException ex) {
      LOGGER.log(Level.FINE, "shutdown error", ex);
    }

    if (metricsProxyConnectionHandler != null) {
      try {
        metricsProxyConnectionHandler.close();
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "error closing metricsProxyConnectionHandler", e);
      }
    }

    if (histogramProxyConnectionHandler != null) {
      try {
        histogramProxyConnectionHandler.close();
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "error closing histogramProxyConnectionHandler", e);
      }
    }

    if (tracingProxyConnectionHandler != null) {
      try {
        tracingProxyConnectionHandler.close();
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "error closing tracingProxyConnectionHandler", e);
      }
    }
  }
}
