package com.wavefront.sdk.common.clients;

import com.wavefront.sdk.common.clients.service.ReportingService;
import com.wavefront.sdk.common.metrics.WavefrontSdkCounter;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the {@link WavefrontClient} class
 *
 * @author Mike McMahon (mike.mcmahon@wavefront.com)
 */
public class WavefrontClientTest {

  @Test
  public void testGetBatch() {
    int batchSize = 8;
    int messageSizeBytes = 200;

    LinkedBlockingQueue<String> buffer = new LinkedBlockingQueue<>();
    buffer.offer(createString(50));   // chunk 1
    buffer.offer(createString(50));   // chunk 1
    buffer.offer(createString(50));   // chunk 1
    buffer.offer(createString(100));  // chunk 2
    buffer.offer(createString(250));  // dropped
    buffer.offer(createString(100));  // chunk 2
    buffer.offer(createString(200));  // chunk 3
    buffer.offer(createString(50));   // chunk 4
    buffer.offer(createString(50));   // chunk 4
    buffer.offer(createString(50));   // remains in buffer

    WavefrontSdkCounter dropped = createMock(WavefrontSdkCounter.class);
    dropped.inc();
    expectLastCall().once();

    replay(dropped);
    List<List<String>> batch = WavefrontClient.getBatch(buffer, batchSize,
        messageSizeBytes, dropped);
    verify(dropped);

    assertEquals(4, batch.size());
    assertEquals(3, batch.get(0).size());
    assertEquals(2, batch.get(1).size());
    assertEquals(1, batch.get(2).size());
    assertEquals(2, batch.get(3).size());
    assertEquals(1, buffer.size());
  }

  private String createString(int size) {
    return new String(new char[size]).replace("\0", "a");
  }

  @Test
  public void testUrlFormatForService() {
    validateURI(URI.create("http://127.0.0.1:2878"), "http://127.0.0.1:2878/report?f=wavefront");
    validateURI(URI.create("http://127.0.0.1:2878/"), "http://127.0.0.1:2878/report?f=wavefront");
    validateURI(URI.create("http://127.0.0.1:2878////"), "http://127.0.0.1:2878/report?f=wavefront");
    validateURI(URI.create("http://localhost:2878/report"), "http://localhost:2878/report?f=wavefront");
    validateURI(URI.create("http://localhost:2878/report/"), "http://localhost:2878/report?f=wavefront");
    validateURI(URI.create("http://corp.proxies.acme.com:2878/prod/report/"),
        "http://corp.proxies.acme.com:2878/prod/report?f=wavefront");
    validateURI(URI.create("https://domain.wavefront.com"), "https://domain.wavefront.com/report?f=wavefront");
    validateURI(URI.create("https://domain.wavefront.com/"), "https://domain.wavefront.com/report?f=wavefront");
    validateURI(URI.create("https://domain.wavefront.com/report/"), "https://domain.wavefront.com/report?f=wavefront");
    validateURI(URI.create("https://domain.wavefront.com/report"), "https://domain.wavefront.com/report?f=wavefront");
  }

  private void validateURI(URI uri, String expected) {
    try {
      URL url = ReportingService.getReportingUrl(uri, "wavefront");
      assertEquals(url.toString(), expected);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testUrlFormatForEvnetService() {
    validateEventURI(URI.create("http://127.0.0.1:2878"), "http://127.0.0.1:2878/api/v2/event");
    validateEventURI(URI.create("http://127.0.0.1:2878/"), "http://127.0.0.1:2878/api/v2/event");
    validateEventURI(URI.create("http://127.0.0.1:2878////"), "http://127.0.0.1:2878/api/v2/event");
    validateEventURI(URI.create("http://localhost:2878/api/v2/event"), "http://localhost:2878/api/v2/event");
    validateEventURI(URI.create("http://localhost:2878/api/v2/event/"), "http://localhost:2878/api/v2/event");
    validateEventURI(URI.create("http://corp.proxies.acme.com:2878/prod/api/v2/event/"),
        "http://corp.proxies.acme.com:2878/prod/api/v2/event");
    validateEventURI(URI.create("https://domain.wavefront.com"), "https://domain.wavefront.com/api/v2/event");
    validateEventURI(URI.create("https://domain.wavefront.com/"), "https://domain.wavefront.com/api/v2/event");
    validateEventURI(URI.create("https://domain.wavefront.com/api/v2/event/"), "https://domain.wavefront.com/api/v2/event");
    validateEventURI(URI.create("https://domain.wavefront.com/api/v2/event"), "https://domain.wavefront.com/api/v2/event");
  }

  private void validateEventURI(URI uri, String expected) {
    try {
      URL url = ReportingService.getEventReportingUrl(uri);
      assertEquals(url.toString(), expected);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
}
