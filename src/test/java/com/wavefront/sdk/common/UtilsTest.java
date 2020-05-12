package com.wavefront.sdk.common;

import com.wavefront.sdk.entities.histograms.HistogramGranularity;
import com.wavefront.sdk.entities.tracing.SpanLog;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.wavefront.sdk.common.Utils.histogramToLineData;
import static com.wavefront.sdk.common.Utils.metricToLineData;
import static com.wavefront.sdk.common.Utils.sanitize;
import static com.wavefront.sdk.common.Utils.sanitizeValue;
import static com.wavefront.sdk.common.Utils.sanitizeWithoutQuotes;
import static com.wavefront.sdk.common.Utils.tracingSpanToLineData;
import static com.wavefront.sdk.common.Utils.spanLogsToLineData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link Utils}
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class UtilsTest {

  @Test
  public void testSanitize() {
    assertEquals("\"hello\"", sanitize("hello"));
    assertEquals("\"hello-world\"", sanitize("hello world"));
    assertEquals("\"hello.world\"", sanitize("hello.world"));
    assertEquals("\"hello-world-\"", sanitize("hello\"world\""));
    assertEquals("\"hello-world\"", sanitize("hello'world"));
    assertEquals("\"hello-world\"", sanitize("hello/world"));
    assertEquals("\"~component.heartbeat\"", sanitize("~component.heartbeat"));
    assertEquals("\"-component.heartbeat\"", sanitize("!component.heartbeat"));
    assertEquals("\"Δcomponent.heartbeat\"", sanitize("Δcomponent.heartbeat"));
    assertEquals("\"∆component.heartbeat\"", sanitize("∆component.heartbeat"));
  }

  @Test
  public void testSanitizeWithoutQuotes() {
    assertEquals("hello-world", sanitizeWithoutQuotes("hello world"));
  }

  @Test
  public void testSanitizeValue() {
    assertEquals("\"hello\"", sanitizeValue("hello"));
    assertEquals("\"hello world\"", sanitizeValue("hello world"));
    assertEquals("\"hello.world\"", sanitizeValue("hello.world"));
    assertEquals("\"hello\\\"world\\\"\"", sanitizeValue("hello\"world\""));
    assertEquals("\"hello'world\"", sanitizeValue("hello'world"));
    assertEquals("\"hello\\nworld\"", sanitizeValue("hello\nworld"));
  }

  @Test
  public void testMetricToLineData() {
    Map<String, String> tags = new HashMap<String, String>() {{
      put("datacenter", "dc1");
    }};
    assertEquals("\"new-york.power.usage\" 42422.0 1493773500 source=\"localhost\" " +
        "\"datacenter\"=\"dc1\"\n", metricToLineData("new-york.power.usage", 42422, 1493773500L,
        "localhost", tags, "defaultSource"));
    // null timestamp
    assertEquals("\"new-york.power.usage\" 42422.0 source=\"localhost\" " +
        "\"datacenter\"=\"dc1\"\n", metricToLineData("new-york.power.usage", 42422, null,
        "localhost", tags, "defaultSource"));
    // null tags
    assertEquals("\"new-york.power.usage\" 42422.0 1493773500 source=\"localhost\"\n",
        metricToLineData("new-york.power.usage", 42422, 1493773500L,
            "localhost", null, "defaultSource"));
    // null tags and null timestamp
    assertEquals("\"new-york.power.usage\" 42422.0 source=\"localhost\"\n",
        metricToLineData("new-york.power.usage", 42422, null, "localhost", null,
            "defaultSource"));
    // default source
    assertEquals("\"new-york.power.usage\" 42422.0 source=\"defaultSource\"\n",
        metricToLineData("new-york.power.usage", 42422, null, null, null, "defaultSource"));
    // Add tag key with invalid char, val with empty space
    tags.put(" key name~1", " val name 1 ");
    // Invalid char in source and metrics
    assertEquals("\"new-york.power.usage\" 42422.0 1493773500 source=\"local-host\" " +
            "\"-key-name-1\"=\"val name 1\" " + "\"datacenter\"=\"dc1\"\n",
        metricToLineData("new~york.power.usage", 42422, 1493773500L, "local~host", tags,
            "defaultSource"));
  }

  @Test
  public void testInvalidMetricToLineDataThrows() {
    Map<String, String> tags = new HashMap<String, String>() {{
      put("datacenter", "dc1");
    }};
    try {
      metricToLineData(null, 42422, 1493773500L, "localhost", tags, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("metrics name cannot be blank"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("datacenter=[dc1]"));
    }
    try {
      metricToLineData("", 42422, 1493773500L, "localhost", tags, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("metrics name cannot be blank"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("datacenter=[dc1]"));
    }
    try {
      metricToLineData("new-york.power.usage", 42422, 1493773500L, null, tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("source cannot be blank"));
      assertTrue(e.getMessage().contains("new-york.power.usage"));
      assertTrue(e.getMessage().contains("datacenter=[dc1]"));
    }
    tags.put("", "value");
    try {
      metricToLineData("new-york.power.usage", 42422, 1493773500L, "localhost", tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("metric point tag key cannot be blank"));
      assertTrue(e.getMessage().contains("new-york.power.usage"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("datacenter=[dc1]"));
    }
    tags.remove("");
    tags.put("emptyValue", null);
    try {
      metricToLineData("new-york.power.usage", 42422, 1493773500L, "localhost", tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("metric point tag value cannot be blank for"));
      assertTrue(e.getMessage().contains("emptyValue=[null]"));
      assertTrue(e.getMessage().contains("new-york.power.usage"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("datacenter=[dc1]"));
    }
    tags.put("emptyValue", "");
    try {
      metricToLineData("new-york.power.usage", 42422, 1493773500L, "localhost", tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("metric point tag value cannot be blank for"));
      assertTrue(e.getMessage().contains("emptyValue=[]"));
      assertTrue(e.getMessage().contains("new-york.power.usage"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("datacenter=[dc1]"));
    }
  }

  @Test
  public void testHistogramToLineData() {
    Map<String, String> tags = new HashMap<String, String>() {{
      put("region", "us-west");
    }};
    Set<HistogramGranularity> minGranularity = new HashSet<>();
    minGranularity.add(HistogramGranularity.MINUTE);
    assertEquals("!M 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\" " +
            "\"region\"=\"us-west\"\n",
        histogramToLineData("request.latency", Arrays.asList(new Pair<>(30.0, 20),
            new Pair<>(5.1, 10)), minGranularity,
            1493773500L, "appServer1", tags, "defaultSource"));

    // null timestamp
    assertEquals("!M #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\" " +
            "\"region\"=\"us-west\"\n",
        histogramToLineData("request.latency", Arrays.asList(new Pair<>(30.0, 20), new Pair<>(5.1, 10)),
            minGranularity, null, "appServer1", tags, "defaultSource"));

    // null tags
    assertEquals("!M 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\"\n",
        histogramToLineData("request.latency", Arrays.asList(new Pair<>(30.0, 20),
            new Pair<>(5.1, 10)),
            minGranularity, 1493773500L, "appServer1", null, "defaultSource"));

    // empty centroids
    try {
      assertEquals("!M 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\"\n",
          histogramToLineData("request.latency", new ArrayList<>(),
              minGranularity, 1493773500L, "appServer1", null, "defaultSource"));
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    // no histogram granularity specified
    try {
      assertEquals("!M 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\"\n",
          histogramToLineData("request.latency", Arrays.asList(new Pair<>(30.0, 20),
              new Pair<>(5.1, 10)), new HashSet<>(),
              1493773500L, "appServer1", null, "defaultSource"));
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    Set<HistogramGranularity> allGranularity = new HashSet<>();
    allGranularity.add(HistogramGranularity.MINUTE);
    allGranularity.add(HistogramGranularity.HOUR);
    allGranularity.add(HistogramGranularity.DAY);
    String[] tmp = histogramToLineData("request.latency",
        Arrays.asList(new Pair<>(30.0, 20), new Pair<>(5.1, 10)),
        allGranularity, 1493773500L, "appServer1", tags, "defaultSource").split("\n");
    Arrays.sort(tmp);
    StringBuilder sb = new StringBuilder();
    for (String item : tmp) {
      sb.append(item);
      sb.append("\n");
    }
    // multiple granularities
    assertEquals("!D 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\" " +
            "\"region\"=\"us-west\"\n" +
            "!H 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\" " +
            "\"region\"=\"us-west\"\n" +
            "!M 1493773500 #20 30.0 #10 5.1 \"request.latency\" source=\"appServer1\" " +
            "\"region\"=\"us-west\"\n",
        sb.toString());
  }

  @Test
  public void testInvalidHistogramToLineDataThrows() {
    Map<String, String> tags = new HashMap<String, String>() {{
      put("region", "us-west");
    }};
    Set<HistogramGranularity> minGranularity = new HashSet<HistogramGranularity>() {{
      add(HistogramGranularity.MINUTE);
    }};
    try {
      histogramToLineData(null, Arrays.asList(new Pair<>(30.0, 20), new Pair<>(5.1, 10)),
          minGranularity, 1493773500L, "appServer1", tags, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("histogram name cannot be blank"));
      assertTrue(e.getMessage().contains("source=appServer1"));
      assertTrue(e.getMessage().contains("region=[us-west]"));
    }
    try {
      histogramToLineData("", Arrays.asList(new Pair<>(30.0, 20), new Pair<>(5.1, 10)),
          minGranularity, 1493773500L, "appServer1", tags, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("histogram name cannot be blank"));
      assertTrue(e.getMessage().contains("source=appServer1"));
      assertTrue(e.getMessage().contains("region=[us-west]"));
    }
    try {
      histogramToLineData("requests.latency", Arrays.asList(new Pair<>(3.0, 2), new Pair<>(5.0, 1)),
          minGranularity, 1493773500L, null, tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("histogram source cannot be blank"));
      assertTrue(e.getMessage().contains("requests.latency"));
      assertTrue(e.getMessage().contains("region=[us-west]"));
    }
    tags.put("", "value");
    try {
      histogramToLineData("requests.latency", Arrays.asList(new Pair<>(3.0, 2), new Pair<>(5.0, 1)),
          minGranularity, 1493773500L, "appServer1", tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("histogram tag key cannot be blank"));
      assertTrue(e.getMessage().contains("requests.latency"));
      assertTrue(e.getMessage().contains("source=appServer1"));
      assertTrue(e.getMessage().contains("region=[us-west]"));
    }
    tags.remove("");
    tags.put("emptyValue", null);
    try {
      histogramToLineData("requests.latency", Arrays.asList(new Pair<>(3.0, 2), new Pair<>(5.0, 1)),
          minGranularity, 1493773500L, "appServer1", tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("histogram tag value cannot be blank for"));
      assertTrue(e.getMessage().contains("emptyValue=[null]"));
      assertTrue(e.getMessage().contains("requests.latency"));
      assertTrue(e.getMessage().contains("source=appServer1"));
      assertTrue(e.getMessage().contains("region=[us-west]"));
    }
    tags.put("emptyValue", "");
    try {
      histogramToLineData("requests.latency", Arrays.asList(new Pair<>(3.0, 2), new Pair<>(5.0, 1)),
          minGranularity, 1493773500L, "appServer1", tags, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("histogram tag value cannot be blank for"));
      assertTrue(e.getMessage().contains("emptyValue=[]"));
      assertTrue(e.getMessage().contains("requests.latency"));
      assertTrue(e.getMessage().contains("source=appServer1"));
      assertTrue(e.getMessage().contains("region=[us-west]"));
    }
  }

  @Test
  public void testTracingSpanToLineData() {
    assertEquals("\"getAllUsers\" source=\"localhost\" " +
            "traceId=7b3bf470-9456-11e8-9eb6-529269fb1459 spanId=0313bafe-9457-11e8-9eb6-529269fb1459 " +
            "parent=2f64e538-9457-11e8-9eb6-529269fb1459 " +
            "followsFrom=5f64e538-9457-11e8-9eb6-529269fb1459 " +
            "\"application\"=\"Wavefront\" " +
            "\"http.method\"=\"GET\" 1493773500 343500\n",
        tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
            UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
                "0313bafe-9457-11e8-9eb6-529269fb1459"),
            Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
            Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
            Arrays.asList(new Pair<>("application", "Wavefront"),
                new Pair<>("http.method", "GET")), null, "defaultSource"));

    // null followsFrom
    assertEquals("\"getAllUsers\" source=\"localhost\" " +
            "traceId=7b3bf470-9456-11e8-9eb6-529269fb1459 spanId=0313bafe-9457-11e8-9eb6-529269fb1459 " +
            "parent=2f64e538-9457-11e8-9eb6-529269fb1459 \"application\"=\"Wavefront\" " +
            "\"http.method\"=\"GET\" 1493773500 343500\n",
        tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
            UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"),
            UUID.fromString("0313bafe-9457-11e8-9eb6-529269fb1459"),
            Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")), null,
            Arrays.asList(new Pair<>("application", "Wavefront"),
                new Pair<>("http.method", "GET")), null, "defaultSource"));

    // root span
    assertEquals("\"getAllUsers\" source=\"localhost\" " +
            "traceId=7b3bf470-9456-11e8-9eb6-529269fb1459 spanId=0313bafe-9457-11e8-9eb6-529269fb1459 " +
            "\"application\"=\"Wavefront\" " +
            "\"http.method\"=\"GET\" 1493773500 343500\n",
        tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
            UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
                "0313bafe-9457-11e8-9eb6-529269fb1459"), null, null,
            Arrays.asList(new Pair<>("application", "Wavefront"),
                new Pair<>("http.method", "GET")), null, "defaultSource"));

    // null tags
    assertEquals("\"getAllUsers\" source=\"localhost\" " +
            "traceId=7b3bf470-9456-11e8-9eb6-529269fb1459 spanId=0313bafe-9457-11e8-9eb6-529269fb1459 " +
            "\"_spanLogs\"=\"true\" " +
            "1493773500 343500\n",
        tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
            UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
                "0313bafe-9457-11e8-9eb6-529269fb1459"), null, null, null, new ArrayList<SpanLog>() {{
              add(new SpanLog(System.currentTimeMillis(), new HashMap<>()));
            }}, "defaultSource"));
  }

  @Test
  public void testInvalidTracingSpanToLineDataThrows() {
    try {
      tracingSpanToLineData(null, 1493773500L, 343500L, "localhost",
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>("application", "Wavefront"),
              new Pair<>("http.method", "GET")), null, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span name cannot be blank"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("application=[Wavefront]"));
      assertTrue(e.getMessage().contains("http.method=[GET]"));
    }
    try {
      tracingSpanToLineData("", 1493773500L, 343500L, "localhost",
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>("application", "Wavefront"),
              new Pair<>("http.method", "GET")), null, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span name cannot be blank"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("application=[Wavefront]"));
      assertTrue(e.getMessage().contains("http.method=[GET]"));
    }
    try {
      tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, null,
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>("application", "Wavefront"),
              new Pair<>("http.method", "GET")), null, "");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span source cannot be blank"));
      assertTrue(e.getMessage().contains("getAllUsers"));
      assertTrue(e.getMessage().contains("application=[Wavefront]"));
      assertTrue(e.getMessage().contains("http.method=[GET]"));
    }
    try {
      tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>(null, "Wavefront"),
              new Pair<>("http.method", "GET")), null, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span tag key cannot be blank"));
      assertTrue(e.getMessage().contains("getAllUsers"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("http.method=[GET]"));
    }
    try {
      tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>("", "Wavefront"),
              new Pair<>("http.method", "GET")), null, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span tag key cannot be blank"));
      assertTrue(e.getMessage().contains("getAllUsers"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("http.method=[GET]"));
    }
    try {
      tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>("application", "Wavefront"),
              new Pair<>("http.method", null)), null, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span tag value cannot be blank"));
      assertTrue(e.getMessage().contains("getAllUsers"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("application=[Wavefront]"));
      assertTrue(e.getMessage().contains("http.method=[null]"));
    }
    try {
      tracingSpanToLineData("getAllUsers", 1493773500L, 343500L, "localhost",
          UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"), UUID.fromString(
              "0313bafe-9457-11e8-9eb6-529269fb1459"),
          Arrays.asList(UUID.fromString("2f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(UUID.fromString("5f64e538-9457-11e8-9eb6-529269fb1459")),
          Arrays.asList(new Pair<>("application", "Wavefront"),
              new Pair<>("http.method", "")), null, "defaultSource");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("span tag value cannot be blank"));
      assertTrue(e.getMessage().contains("getAllUsers"));
      assertTrue(e.getMessage().contains("source=localhost"));
      assertTrue(e.getMessage().contains("application=[Wavefront]"));
      assertTrue(e.getMessage().contains("http.method=[]"));
    }
  }

  @Test
  public void testSpanLogsToLineData() throws IOException {
    assertEquals("{\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
            "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
            "\"logs\":[{\"timestamp\":91616745187,\"fields\":{\"key1\":\"val1\"}}]," +
            "\"span\":null}\n",
        spanLogsToLineData(UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"),
            UUID.fromString("0313bafe-9457-11e8-9eb6-529269fb1459"),
            Arrays.asList(new SpanLog(91616745187L,
                new HashMap<String, String>() {{
                  put("key1", "val1");
                }}))));
  }

  @Test
  public void testSpanLogsToLineDataWithSpan() throws IOException {
    assertEquals("{\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
            "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
            "\"logs\":[{\"timestamp\":91616745187,\"fields\":{\"key1\":\"val1\"}}]," +
            "\"span\":\"\\\"getAllUsers\\\" source=\\\"localhost\\\" " +
              "traceId=7b3bf470-9456-11e8-9eb6-529269fb1459 " +
              "spanId=0313bafe-9457-11e8-9eb6-529269fb1459 " +
              "parent=2f64e538-9457-11e8-9eb6-529269fb1459 " +
              "followsFrom=5f64e538-9457-11e8-9eb6-529269fb1459 " +
              "\\\"application\\\"=\\\"Wavefront\\\" \\\"http.method\\\"=\\\"GET\\\" " +
              "1493773500 343500\\n\"" +
            "}\n",
        spanLogsToLineData(UUID.fromString("7b3bf470-9456-11e8-9eb6-529269fb1459"),
            UUID.fromString("0313bafe-9457-11e8-9eb6-529269fb1459"),
            Arrays.asList(new SpanLog(91616745187L,
                new HashMap<String, String>() {{
                  put("key1", "val1");
                }})),
            "\"getAllUsers\" source=\"localhost\" " +
                "traceId=7b3bf470-9456-11e8-9eb6-529269fb1459 " +
                "spanId=0313bafe-9457-11e8-9eb6-529269fb1459 " +
                "parent=2f64e538-9457-11e8-9eb6-529269fb1459 " +
                "followsFrom=5f64e538-9457-11e8-9eb6-529269fb1459 " +
                "\"application\"=\"Wavefront\" " +
                "\"http.method\"=\"GET\" 1493773500 343500\n"));
  }
}
