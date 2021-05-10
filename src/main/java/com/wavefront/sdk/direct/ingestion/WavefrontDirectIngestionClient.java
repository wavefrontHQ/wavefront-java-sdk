package com.wavefront.sdk.direct.ingestion;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wavefront.sdk.common.Constants;
import com.wavefront.sdk.common.NamedThreadFactory;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.Utils;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.common.annotation.Nullable;
import com.wavefront.sdk.common.clients.WavefrontClientFactory;
import com.wavefront.sdk.common.logging.MessageDedupingLogger;
import com.wavefront.sdk.common.metrics.WavefrontSdkDeltaCounter;
import com.wavefront.sdk.common.metrics.WavefrontSdkMetricsRegistry;
import com.wavefront.sdk.entities.histograms.HistogramGranularity;
import com.wavefront.sdk.entities.tracing.SpanLog;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.sdk.common.Utils.histogramToLineData;
import static com.wavefront.sdk.common.Utils.metricToLineData;
import static com.wavefront.sdk.common.Utils.spanLogsToLineData;
import static com.wavefront.sdk.common.Utils.tracingSpanToLineData;

/**
 * Wavefront direct ingestion client that sends data directly to Wavefront cluster via the direct ingestion API.
 *
 * @deprecated This class will be removed in future versions in favor of
 * {@link WavefrontClientFactory} to construct Proxy and DirectDataIngestion senders.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
@Deprecated
public class WavefrontDirectIngestionClient implements WavefrontSender, Runnable {

  // Limit identical log messages to at most once every 5 seconds
  private static final MessageDedupingLogger logger = new MessageDedupingLogger(Logger.getLogger(
      WavefrontDirectIngestionClient.class.getCanonicalName()), LogMessageType.values().length,
      0.2);

  /**
   * Source to use if entity source is null
   */
  private final String defaultSource;
  private final String clientId;

  private final int batchSize;
  private final int messageSizeBytes;
  private final LinkedBlockingQueue<String> metricsBuffer;
  private final LinkedBlockingQueue<String> histogramsBuffer;
  private final LinkedBlockingQueue<String> tracingSpansBuffer;
  private final LinkedBlockingQueue<String> spanLogsBuffer;
  private final DataIngesterAPI directService;
  private final ScheduledExecutorService scheduler;
  private final WavefrontSdkMetricsRegistry sdkMetricsRegistry;

  // Internal point metrics
  private final WavefrontSdkDeltaCounter pointsValid;
  private final WavefrontSdkDeltaCounter pointsInvalid;
  private final WavefrontSdkDeltaCounter pointsDropped;
  private final WavefrontSdkDeltaCounter pointReportErrors;

  // Internal histogram metrics
  private final WavefrontSdkDeltaCounter histogramsValid;
  private final WavefrontSdkDeltaCounter histogramsInvalid;
  private final WavefrontSdkDeltaCounter histogramsDropped;
  private final WavefrontSdkDeltaCounter histogramReportErrors;

  // Internal tracing span metrics
  private final WavefrontSdkDeltaCounter spansValid;
  private final WavefrontSdkDeltaCounter spansInvalid;
  private final WavefrontSdkDeltaCounter spansDropped;
  private final WavefrontSdkDeltaCounter spanReportErrors;

  // Internal span log metrics
  private final WavefrontSdkDeltaCounter spanLogsValid;
  private final WavefrontSdkDeltaCounter spanLogsInvalid;
  private final WavefrontSdkDeltaCounter spanLogsDropped;
  private final WavefrontSdkDeltaCounter spanLogReportErrors;

  // Consider the feature to be enabled when value is 0, and disabled otherwise
  private final AtomicInteger metricsDisabledStatusCode;
  private final AtomicInteger histogramsDisabledStatusCode;
  private final AtomicInteger spansDisabledStatusCode;
  private final AtomicInteger spanLogsDisabledStatusCode;

  // Flag to prevent sending after close() has been called
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public static class Builder {
    // Required parameters
    private final String server;
    private final String token;

    // Optional parameters
    private int maxQueueSize = 50000;
    private int batchSize = 10000;
    private int flushIntervalSeconds = 1;
    private int messageSizeBytes = Integer.MAX_VALUE;

    /**
     * Create a new WavefrontDirectIngestionClient.Builder
     *
     * @param server A Wavefront server URL of the form "https://clusterName.wavefront.com"
     * @param token  A valid API token with direct ingestion permissions
     */
    public Builder(String server, String token) {
      this.server = server;
      this.token = token;
    }

    /**
     * Set max queue size of in-memory buffer. Needs to be flushed if full.
     *
     * @param maxQueueSize Max queue size of in-memory buffer
     * @return {@code this}
     */
    public Builder maxQueueSize(int maxQueueSize) {
      this.maxQueueSize = maxQueueSize;
      return this;
    }

    /**
     * Set batch size to be reported during every flush.
     *
     * @param batchSize Batch size to be reported during every flush.
     * @return {@code this}
     */
    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set interval at which you want to flush points to Wavefront cluster.
     *
     * @param flushIntervalSeconds Interval at which you want to flush points to Wavefront cluster
     * @return {@code this}
     */
    public Builder flushIntervalSeconds(int flushIntervalSeconds) {
      this.flushIntervalSeconds = flushIntervalSeconds;
      return this;
    }

    /**
     * Set max message size, such that each batch is reported as one or more messages where no
     * message exceeds the specified size in bytes. The default message size is
     * {@link Integer#MAX_VALUE}.
     *
     * @param bytes Maximum number of bytes of data that each reported message contains.
     * @return {@code this}
     */
    public Builder messageSizeBytes(int bytes) {
      this.messageSizeBytes = bytes;
      return this;
    }

    /**
     * Creates a new client that connects directly to a given Wavefront service.
     *
     * return {@link WavefrontDirectIngestionClient}
     */
    public WavefrontDirectIngestionClient build() {
      return new WavefrontDirectIngestionClient(this);
    }
  }

  private WavefrontDirectIngestionClient(Builder builder) {
    String tempSource = "unknown";
    try {
      tempSource = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      logger.log(LogMessageType.UNKNOWN_HOST.toString(), Level.WARNING,
          "Unable to resolve local host name. Source will default to 'unknown'");
    }
    defaultSource = tempSource;

    batchSize = builder.batchSize;
    messageSizeBytes = builder.messageSizeBytes;
    metricsBuffer = new LinkedBlockingQueue<>(builder.maxQueueSize);
    histogramsBuffer = new LinkedBlockingQueue<>(builder.maxQueueSize);
    tracingSpansBuffer = new LinkedBlockingQueue<>(builder.maxQueueSize);
    spanLogsBuffer = new LinkedBlockingQueue<>(builder.maxQueueSize);
    directService = new DataIngesterService(builder.server, builder.token);
    scheduler = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("wavefrontDirectSender").setDaemon(true));
    scheduler.scheduleAtFixedRate(this, 1, builder.flushIntervalSeconds, TimeUnit.SECONDS);

    String processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    sdkMetricsRegistry = new WavefrontSdkMetricsRegistry.Builder(this).
        prefix(Constants.SDK_METRIC_PREFIX + ".core.sender.direct").
        tag(Constants.PROCESS_TAG_KEY, processId).
        build();

    sdkMetricsRegistry.newGauge("points.queue.size", metricsBuffer::size);
    sdkMetricsRegistry.newGauge("points.queue.remaining_capacity",
        metricsBuffer::remainingCapacity);
    pointsValid = sdkMetricsRegistry.newDeltaCounter("points.valid");
    pointsInvalid = sdkMetricsRegistry.newDeltaCounter("points.invalid");
    pointsDropped = sdkMetricsRegistry.newDeltaCounter("points.dropped");
    pointReportErrors = sdkMetricsRegistry.newDeltaCounter("points.report.errors");

    sdkMetricsRegistry.newGauge("histograms.queue.size", histogramsBuffer::size);
    sdkMetricsRegistry.newGauge("histograms.queue.remaining_capacity",
        histogramsBuffer::remainingCapacity);
    histogramsValid = sdkMetricsRegistry.newDeltaCounter("histograms.valid");
    histogramsInvalid = sdkMetricsRegistry.newDeltaCounter("histograms.invalid");
    histogramsDropped = sdkMetricsRegistry.newDeltaCounter("histograms.dropped");
    histogramReportErrors = sdkMetricsRegistry.newDeltaCounter("histograms.report.errors");

    sdkMetricsRegistry.newGauge("spans.queue.size", tracingSpansBuffer::size);
    sdkMetricsRegistry.newGauge("spans.queue.remaining_capacity",
        tracingSpansBuffer::remainingCapacity);
    spansValid = sdkMetricsRegistry.newDeltaCounter("spans.valid");
    spansInvalid = sdkMetricsRegistry.newDeltaCounter("spans.invalid");
    spansDropped = sdkMetricsRegistry.newDeltaCounter("spans.dropped");
    spanReportErrors = sdkMetricsRegistry.newDeltaCounter("spans.report.errors");

    sdkMetricsRegistry.newGauge("span_logs.queue.size", spanLogsBuffer::size);
    sdkMetricsRegistry.newGauge("span_logs.queue.remaining_capacity",
        spanLogsBuffer::remainingCapacity);
    spanLogsValid = sdkMetricsRegistry.newDeltaCounter("span_logs.valid");
    spanLogsInvalid = sdkMetricsRegistry.newDeltaCounter("span_logs.invalid");
    spanLogsDropped = sdkMetricsRegistry.newDeltaCounter("span_logs.dropped");
    spanLogReportErrors = sdkMetricsRegistry.newDeltaCounter("span_logs.report.errors");

    metricsDisabledStatusCode = new AtomicInteger();
    histogramsDisabledStatusCode = new AtomicInteger();
    spansDisabledStatusCode = new AtomicInteger();
    spanLogsDisabledStatusCode = new AtomicInteger();

    this.clientId = builder.server;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public void sendMetric(String name, double value, @Nullable Long timestamp,
                         @Nullable String source, @Nullable Map<String, String> tags)
      throws IOException {
    if (closed.get()) {
      throw new IOException("attempt to send using closed sender");
    }
    String point;
    try {
      point = metricToLineData(name, value, timestamp, source, tags, defaultSource);
      pointsValid.inc();
    } catch (IllegalArgumentException e) {
      pointsInvalid.inc();
      throw e;
    }

    if (!metricsBuffer.offer(point)) {
      pointsDropped.inc();
      logger.log(LogMessageType.METRICS_BUFFER_FULL.toString(), Level.WARNING,
          "Buffer full, dropping metric point: " + point + ". Consider increasing the batch size " +
              "of your sender to increase throughput.");
    }
  }

  @Override
  public void sendLog(String name, double value, Long timestamp, String source, Map<String, String> tags) {
    throw new IllegalArgumentException("Sending logs using this method is not allowed");
  }

  @Override
  public void sendFormattedMetric(String point) throws IOException {
    if (closed.get()) {
      throw new IOException("attempt to send using closed sender");
    }
    if (point == null || "".equals(point.trim())) {
      pointsInvalid.inc();
      throw new IllegalArgumentException("point must be non-null and in WF data format");
    }
    pointsValid.inc();
    String finalPoint = point.endsWith("\n") ? point : point + "\n";

    if (!metricsBuffer.offer(finalPoint)) {
      pointsDropped.inc();
      logger.log(LogMessageType.METRICS_BUFFER_FULL.toString(), Level.WARNING,
          "Buffer full, dropping metric point: " + finalPoint + ". Consider increasing the batch " +
              "size of your sender to increase throughput.");
    }
  }

  @Override
  public void sendDistribution(String name, List<Pair<Double, Integer>> centroids,
                               Set<HistogramGranularity> histogramGranularities,
                               @Nullable Long timestamp, @Nullable String source,
                               @Nullable Map<String, String> tags)
      throws IOException {
    if (closed.get()) {
      throw new IOException("attempt to send using closed sender");
    }
    String histograms;
    try {
      histograms = histogramToLineData(name, centroids, histogramGranularities, timestamp,
          source, tags, defaultSource);
      histogramsValid.inc();
    } catch (IllegalArgumentException e) {
      histogramsInvalid.inc();
      throw e;
    }

    if (!histogramsBuffer.offer(histograms)) {
      histogramsDropped.inc();
      logger.log(LogMessageType.HISTOGRAMS_BUFFER_FULL.toString(), Level.WARNING,
          "Buffer full, dropping histograms: " + histograms + ". Consider increasing the batch " +
              "size of your sender to increase throughput.");
    }
  }

  @Override
  public void sendSpan(String name, long startMillis, long durationMillis,
                       @Nullable String source, UUID traceId, UUID spanId,
                       @Nullable List<UUID> parents, @Nullable List<UUID> followsFrom,
                       @Nullable List<Pair<String, String>> tags, @Nullable List<SpanLog> spanLogs)
      throws IOException {
    if (closed.get()) {
      throw new IOException("attempt to send using closed sender");
    }
    String span;
    try {
      span = tracingSpanToLineData(name, startMillis, durationMillis, source, traceId,
          spanId, parents, followsFrom, tags, spanLogs, defaultSource);
      spansValid.inc();
    } catch (IllegalArgumentException e) {
      spansInvalid.inc();
      throw e;
    }

    if (tracingSpansBuffer.offer(span)) {
      // attempt span logs after span is sent.
      if (spanLogs != null && !spanLogs.isEmpty()) {
        sendSpanLogs(traceId, spanId, spanLogs, span);
      }
    } else {
      spansDropped.inc();
      if (spanLogs != null && !spanLogs.isEmpty()) {
        spanLogsDropped.inc();
      }
      logger.log(LogMessageType.SPANS_BUFFER_FULL.toString(), Level.WARNING,
          "Buffer full, dropping span: " + span + ". Consider increasing the batch size of your " +
              "sender to increase throughput.");
    }
  }

  private void sendSpanLogs(UUID traceId, UUID spanId, List<SpanLog> spanLogs, String span) {
    // attempt span logs
    try {
      String spanLogsJson = spanLogsToLineData(traceId, spanId, spanLogs, span);
      spanLogsValid.inc();
      if (!spanLogsBuffer.offer(spanLogsJson)) {
        spanLogsDropped.inc();
        logger.log(LogMessageType.SPANLOGS_BUFFER_FULL.toString(), Level.WARNING,
            "Buffer full, dropping spanLogs: " + spanLogsJson + ". Consider increasing the batch " +
                "size of your sender to increase throughput.");
      }
    } catch (JsonProcessingException e) {
      spanLogsInvalid.inc();
      logger.log(LogMessageType.SPANLOGS_PROCESSING_ERROR.toString(), Level.WARNING,
          "Unable to serialize span logs to JSON: traceId=" + traceId + " spanId=" + spanId +
              " spanLogs=" + spanLogs);
    }
  }

  public void sendEvent(String name, long startMillis, long endMillis, @Nullable String source,
                        @Nullable Map<String, String> tags,
                        @Nullable Map<String, String> annotations)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Sending event is not supported for " +
        "deprecated WavefrontDirectIngestionClient. Please use WavefrontClient to send events.");
  }

  @Override
  public void run() {
    try {
      this.flush();
    } catch (Throwable ex) {
      logger.log(LogMessageType.FLUSH_ERROR.toString(), Level.WARNING,
          "Unable to report to Wavefront cluster: " + Throwables.getRootCause(ex));
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed.get()) {
      throw new IOException("attempt to flush closed sender");
    }
    this.flushNoCheck();
  }

  private void flushNoCheck() throws IOException {
    internalFlush(metricsBuffer, Constants.WAVEFRONT_METRIC_FORMAT, "points", "points",
        pointsDropped, pointReportErrors, metricsDisabledStatusCode,
        LogMessageType.SEND_METRICS_ERROR, LogMessageType.SEND_METRICS_PERMISSIONS,
        LogMessageType.METRICS_BUFFER_FULL);
    internalFlush(histogramsBuffer, Constants.WAVEFRONT_HISTOGRAM_FORMAT, "histograms",
        "histograms", histogramsDropped, histogramReportErrors, histogramsDisabledStatusCode,
        LogMessageType.SEND_HISTOGRAMS_ERROR, LogMessageType.SEND_HISTOGRAMS_PERMISSIONS,
        LogMessageType.HISTOGRAMS_BUFFER_FULL);
    internalFlush(tracingSpansBuffer, Constants.WAVEFRONT_TRACING_SPAN_FORMAT, "spans", "spans",
        spansDropped, spanReportErrors, spansDisabledStatusCode, LogMessageType.SEND_SPANS_ERROR,
        LogMessageType.SEND_SPANS_PERMISSIONS, LogMessageType.SPANS_BUFFER_FULL);
    internalFlush(spanLogsBuffer, Constants.WAVEFRONT_SPAN_LOG_FORMAT, "span_logs", "span logs",
        spanLogsDropped, spanLogReportErrors, spanLogsDisabledStatusCode,
        LogMessageType.SEND_SPANLOGS_ERROR, LogMessageType.SEND_SPANLOGS_PERMISSIONS,
        LogMessageType.SPANLOGS_BUFFER_FULL);
  }

  private void internalFlush(LinkedBlockingQueue<String> buffer, String format,
                             String entityPrefix, String entityType,
                             WavefrontSdkDeltaCounter dropped, WavefrontSdkDeltaCounter reportErrors,
                             AtomicInteger featureDisabledStatusCode,
                             LogMessageType errorMessageType,
                             LogMessageType permissionsMessageType,
                             LogMessageType bufferFullMessageType)
      throws IOException {
    List<List<String>> batch = getBatch(buffer, batchSize, messageSizeBytes, dropped);
    for (int i = 0; i < batch.size(); i++) {
      List<String> items = batch.get(i);
      int featureDisabledReason = featureDisabledStatusCode.get();
      if (featureDisabledReason != 0) {
        switch (featureDisabledReason) {
          case 401:
            logger.log(permissionsMessageType.toString(), Level.SEVERE,
                "Please verify that your API Token is correct! All " + entityType + " will be " +
                    "discarded until the service is restarted.");
            break;
          case 403:
            if (format.equals(Constants.WAVEFRONT_METRIC_FORMAT)) {
              logger.log(permissionsMessageType.toString(), Level.SEVERE,
                  "Please verify that Direct Data Ingestion is enabled for your account! All "
                      + entityType + " will be discarded until the service is restarted.");
            } else {
              logger.log(permissionsMessageType.toString(), Level.SEVERE,
                  "Please verify that Direct Data Ingestion and " + entityType + " are " +
                      "enabled for your account! All " + entityType + " will be discarded " +
                      "until the service is restarted.");
            }
        }
        continue;
      }
      try (InputStream is = itemsToStream(items)) {
        int statusCode = directService.report(format, is);
        sdkMetricsRegistry.newDeltaCounter(entityPrefix + ".report." + statusCode).inc();
        if ((400 <= statusCode && statusCode <= 599) || statusCode == -1) {
          switch (statusCode) {
            case 401:
              logger.log(permissionsMessageType.toString(), Level.SEVERE,
                  "Error sending " + entityType + " to Wavefront (HTTP " + statusCode + "). " +
                      "Please verify that your API Token is correct! All " + entityType + " will " +
                      "be discarded until the service is restarted.");
              featureDisabledStatusCode.set(statusCode);
              dropped.inc(items.size());
              break;
            case 403:
              if (format.equals(Constants.WAVEFRONT_METRIC_FORMAT)) {
                logger.log(permissionsMessageType.toString(), Level.SEVERE,
                    "Error sending " + entityType + " to Wavefront (HTTP " + statusCode + "). " +
                        "Please verify that Direct Data Ingestion is enabled for your account! " +
                        "All " + entityType + " will be discarded until the service is restarted.");
              } else {
                logger.log(permissionsMessageType.toString(), Level.SEVERE,
                    "Error sending " + entityType + " to Wavefront (HTTP " + statusCode + "). " +
                        "Please verify that Direct Data Ingestion and " + entityType + " are " +
                        "enabled for your account! All " + entityType + " will be discarded until" +
                        " the service is restarted.");
              }
              featureDisabledStatusCode.set(statusCode);
              dropped.inc(items.size());
              break;
            default:
              logger.log(errorMessageType.toString(), Level.WARNING,
                  "Error sending " + entityType + " to Wavefront (HTTP " + statusCode + "). Data " +
                      "will be requeued and resent.");
              requeue(buffer, items, dropped, entityType, bufferFullMessageType);
          }
        }
      } catch (IOException ex) {
        dropped.inc(items.size());
        reportErrors.inc();
        for (int j = i + 1; j < batch.size(); j++) {
          dropped.inc(batch.get(j).size());
        }
        throw ex;
      }
    }
  }

  private void requeue(LinkedBlockingQueue<String> buffer, List<String> items,
                       WavefrontSdkDeltaCounter dropped, String entityType,
                       LogMessageType bufferFullMessageType) {
    int numAddedBackToBuffer = 0;
    for (String item : items) {
      if (buffer.offer(item)) {
        numAddedBackToBuffer++;
      } else {
        int numDropped = items.size() - numAddedBackToBuffer;
        dropped.inc(numDropped);
        logger.log(bufferFullMessageType.toString(), Level.WARNING,
            "Buffer full, dropping " + numDropped + " " + entityType + ". Consider increasing " +
                "the batch size of your sender to increase throughput.");
        break;
      }
    }
  }

  private InputStream itemsToStream(List<String> items) {
    StringBuilder sb = new StringBuilder();
    for (String item : items) {
      // every line item ends with \n
      sb.append(item);
    }
    return new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public int getFailureCount() {
    return (int) (pointReportErrors.count() + histogramReportErrors.count() +
        spanReportErrors.count());
  }

  @Override
  public synchronized void close() {
    if (!closed.compareAndSet(false, true)) {
      logger.log(LogMessageType.CLOSE_WHILE_CLOSED.toString(), Level.FINE,
          "attempt to close already closed sender");
    }
    // Flush before closing
    try {
      flushNoCheck();
    } catch (IOException e) {
      logger.log(LogMessageType.FLUSH_ERROR.toString(), Level.WARNING,
          "error flushing buffer: " + Throwables.getRootCause(e));
    }

    sdkMetricsRegistry.close();

    try {
      Utils.shutdownExecutorAndWait(scheduler);
    } catch (SecurityException ex) {
      logger.log(LogMessageType.SHUTDOWN_ERROR.toString(), Level.WARNING,
          "shutdown error: " + Throwables.getRootCause(ex));
    }
  }

  /**
   * Dequeue and return a batch of at most N items from buffer (where N = batchSize), broken into
   * chunks where each chunk has at most M bytes of data (where M = messageSizeBytes).
   *
   * Visible for testing.
   *
   * @param buffer            The buffer queue to retrieve items from.
   * @param batchSize         The maximum number of items to retrieve from the buffer.
   * @param messageSizeBytes  The maximum number of bytes in each chunk.
   * @param dropped           A delta counter counting the number of items that are dropped.
   * @return A batch of items retrieved from buffer.
   */
  static List<List<String>> getBatch(LinkedBlockingQueue<String> buffer, int batchSize,
                                     int messageSizeBytes, WavefrontSdkDeltaCounter dropped) {
    batchSize = Math.min(buffer.size(), batchSize);
    List<List<String>> batch = new ArrayList<>();
    List<String> chunk = new ArrayList<>();
    int numBytesInChunk = 0;
    int count = 0;

    while (count < batchSize) {
      String item = buffer.poll();
      if (item == null) {
        break;
      }
      int numBytes = item.getBytes(StandardCharsets.UTF_8).length;
      if (numBytes > messageSizeBytes) {
        logger.log(LogMessageType.MESSAGE_SIZE_LIMIT_EXCEEDED.toString(), Level.WARNING,
            "Dropping data larger than " + messageSizeBytes + " bytes: " + item + ". Consider " +
                "increasing the message size limit of your sender.");
        dropped.inc();
        continue;
      }
      if (numBytesInChunk + numBytes > messageSizeBytes) {
        if (!chunk.isEmpty()) {
          batch.add(chunk);
        }
        chunk = new ArrayList<>();
        numBytesInChunk = 0;
      }
      chunk.add(item);
      numBytesInChunk += numBytes;
      count++;
    }
    if (!chunk.isEmpty()) {
      batch.add(chunk);
    }

    return batch;
  }

  private enum LogMessageType {
    UNKNOWN_HOST,
    METRICS_BUFFER_FULL,
    HISTOGRAMS_BUFFER_FULL,
    SPANS_BUFFER_FULL,
    SPANLOGS_BUFFER_FULL,
    SPANLOGS_PROCESSING_ERROR,
    FLUSH_ERROR,
    CLOSE_WHILE_CLOSED,
    SEND_METRICS_ERROR,
    SEND_HISTOGRAMS_ERROR,
    SEND_SPANS_ERROR,
    SEND_SPANLOGS_ERROR,
    SEND_METRICS_PERMISSIONS,
    SEND_HISTOGRAMS_PERMISSIONS,
    SEND_SPANS_PERMISSIONS,
    SEND_SPANLOGS_PERMISSIONS,
    SHUTDOWN_ERROR,
    MESSAGE_SIZE_LIMIT_EXCEEDED
  }
}
