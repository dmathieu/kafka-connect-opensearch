package com.dmathieu.kafka.opensearch.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import com.github.tomakehurst.wiremock.WireMockServer;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.lessThan;
import static com.github.tomakehurst.wiremock.client.WireMock.moreThanOrExactly;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class OpenSearchConnectorNetworkIT extends BaseConnectorIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int NUM_RECORDS = 5;
  private static final int TASKS_MAX = 1;

  private static final String CONNECTOR_NAME = "es-connector";
  private static final String TOPIC = "test";
  private Map<String, String> props;
  protected WireMockServer wireMockServer;

  @BeforeEach
  public void setup() {
    startConnect();
    connect.kafka().createTopic(TOPIC);

    wireMockServer = new WireMockServer(options().dynamicPort());
    wireMockServer.start();
    wireMockServer.stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));

    props = createProps();
  }

  @AfterEach
  public void cleanup() {
    stopConnect();
    wireMockServer.stop();
  }

  @Test
  public void testRetry() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .inScenario("bulkRetry1")
            .whenScenarioStateIs(Scenario.STARTED)
            .withRequestBody(containing("{\"doc_num\":0}"))
            .willReturn(aResponse().withStatus(500))
            .willSetStateTo("Failed"));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .inScenario("bulkRetry1")
            .whenScenarioStateIs("Failed")
            .withRequestBody(containing("{\"doc_num\":0}"))
            .willSetStateTo("Fixed")
            .willReturn(okJson(errorBulkResponse())));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(4);

    await().untilAsserted(
            () -> assertThat(wireMockServer.getAllScenarios().getScenarios().get(0).getState())
                    .isEqualTo("Fixed"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("RUNNING");
  }

  @Test @Disabled
  public void testConcurrentRequests() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse())
                    .withTransformers(BlockingTransformer.NAME)));
    wireMockServer.stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));

    props.put(CONNECTION_URL_CONFIG, wireMockServer.url("/"));
    props.put(READ_TIMEOUT_MS_CONFIG, "60000");
    props.put(MAX_RETRIES_CONFIG, "0");
    props.put(LINGER_MS_CONFIG, "60000");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "4");

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(10);

    /*BlockingTransformer blockingTransformer = BlockingTransformer.getInstance(wireMockServer);

    // TODO MAX_IN_FLIGHT_REQUESTS_CONFIG is misleading (it allows 1 less concurrent request
    // than configure), but fixing it would be a breaking change.
    // Consider allowing 0 (blocking) and removing "-1"
    await().untilAsserted(() -> {
      assertThat(blockingTransformer.queueLength()).isEqualTo(3);
    });

    blockingTransformer.release(10);

    await().untilAsserted(() -> assertThat(blockingTransformer.requestCount()).isEqualTo(10));*/
  }

  @Test
  public void testReadTimeout() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(ok().withFixedDelay(2_000)));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(NUM_RECORDS);

    // Connector should fail since the request takes longer than request timeout
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("Failed to execute bulk request due to 'java.net.SocketTimeoutException: " +
                    "1,000 milliseconds timeout on connection")
            .contains("after 3 attempt(s)");

    // 1 + 2 retries
    wireMockServer.verify(3, postRequestedFor(urlPathEqualTo("/_bulk")));
  }

  @Test
  public void testTooManyRequests() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(aResponse()
                    .withStatus(429)
                    .withHeader(CONTENT_TYPE, "application/json")
                    .withBody("{\n" +
                    "  \"error\": {\n" +
                    "    \"type\": \"circuit_breaking_exception\",\n" +
                    "    \"reason\": \"Data too large\",\n" +
                    "    \"bytes_wanted\": 123848638,\n" +
                    "    \"bytes_limit\": 123273216,\n" +
                    "    \"durability\": \"TRANSIENT\"\n" +
                    "  },\n" +
                    "  \"status\": 429\n" +
                    "}")));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(NUM_RECORDS);

    // Connector should fail since the request takes longer than request timeout
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("Failed to execute bulk request due to 'ElasticsearchStatusException" +
                    "[Elasticsearch exception [type=circuit_breaking_exception, " +
                    "reason=Data too large]]' after 3 attempt(s)");

    // 1 + 2 retries
    wireMockServer.verify(3, postRequestedFor(urlPathEqualTo("/_bulk")));
  }

  @Test
  public void testServiceUnavailable() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(aResponse()
                    .withStatus(503)));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(NUM_RECORDS);

    // Connector should fail since the request takes longer than request timeout
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("[HTTP/1.1 503 Service Unavailable]")
            .contains("after 3 attempt(s)");

    // 1 + 2 retries
    wireMockServer.verify(3, postRequestedFor(urlPathEqualTo("/_bulk")));
  }

  /**
   * Verify that we apply backpressure (by pausing partitions) if number of offset
   * entries grows too large.
   */
  @Test @Disabled
  public void testPausePartitions() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse())
            .withTransformers(BlockingTransformer.NAME)));
    wireMockServer.stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));

    props.put(CONNECTION_URL_CONFIG, wireMockServer.url("/"));
    props.put(READ_TIMEOUT_MS_CONFIG, "600000");
    props.put(LINGER_MS_CONFIG, "600000");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "600000");
    props.put(MAX_RETRIES_CONFIG, "0");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "4");


    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // First batch should get stuck
    writeRecords(1);

    /*BlockingTransformer blockingTransformer = BlockingTransformer.getInstance(wireMockServer);
    await().untilAsserted(() -> assertThat(blockingTransformer.queueLength()).isEqualTo(1));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse())));

    // Now we write multiple records to hit the limit of offset entries and force
    // partitions to be paused
    writeRecords(1_000);

    await().untilAsserted(() ->
            wireMockServer.verify(moreThanOrExactly(99),
                    postRequestedFor(urlPathEqualTo("/_bulk"))));

    // verify it stays blocked for a couple of seconds
    await().pollDelay(Duration.ofSeconds(2))
            .untilAsserted(() ->
                    wireMockServer.verify(lessThan(1_001),
                    postRequestedFor(urlPathEqualTo("/_bulk"))));

    // Unblock first batch, partitions should be resumed and all records processed
    blockingTransformer.release(1);

    await().untilAsserted(() ->
            wireMockServer.verify(moreThanOrExactly(1_001),
                    postRequestedFor(urlPathEqualTo("/_bulk"))));*/
  }

  /**
   * Verify that connector properly fails while partitions are paused because number of offset
   * entries grew too large.
   */
  @Test @Disabled
  public void testPausePartitionsAndFail() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(aResponse()
                    .withStatus(500)
                    .withTransformers(BlockingTransformer.NAME)));
    wireMockServer.stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));

    props.put(CONNECTION_URL_CONFIG, wireMockServer.url("/"));
    props.put(READ_TIMEOUT_MS_CONFIG, "600000");
    props.put(LINGER_MS_CONFIG, "600000");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "600000");
    props.put(MAX_RETRIES_CONFIG, "0");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "4");

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // First batch should get stuck
    writeRecords(1);

    //BlockingTransformer blockingTransformer = BlockingTransformer.getInstance(wireMockServer);
    //await().untilAsserted(() -> assertThat(blockingTransformer.queueLength()).isEqualTo(1));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse())));

    // Now we write multiple records to hit the limit of offset entries and force
    // partitions to be paused
    writeRecords(1_000);

    await().untilAsserted(() ->
            wireMockServer.verify(moreThanOrExactly(99),
                    postRequestedFor(urlPathEqualTo("/_bulk"))));

    // verify it stays blocked for a couple of seconds
    await().pollDelay(Duration.ofSeconds(2))
            .untilAsserted(() ->
                    wireMockServer.verify(lessThan(1_001),
                            postRequestedFor(urlPathEqualTo("/_bulk"))));

    // Unblock first batch, it should fail
    //blockingTransformer.release(1);

    await().untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));
    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("status line [HTTP/1.1 500 Server Error]");
  }

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();

    // generic configs
    props.put(CONNECTOR_CLASS_CONFIG, OpenSearchSinkConnector.class.getName());
    props.put(TOPICS_CONFIG, TOPIC);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");

    // connectors specific
    props.put(CONNECTION_URL_CONFIG, wireMockServer.url("/"));
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");

    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "60000");
    props.put(BATCH_SIZE_CONFIG, "4");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");

    return props;
  }

  protected void writeRecords(int numRecords) {
    for (int i = 0; i < numRecords; i++) {
      connect.kafka().produce(TOPIC, String.valueOf(i), String.format("{\"doc_num\":%d}", i));
    }
  }

  public static String errorBulkResponse() throws JsonProcessingException {
    return errorBulkResponse(1);
  }

  public static String errorBulkResponse(int items) throws JsonProcessingException {
    ObjectNode response = MAPPER.createObjectNode();
    ArrayNode itemsArray = response
            .put("errors", false)
            .putArray("items");

    for (int i = 0; i < items; i++) {
      itemsArray
              .addObject()
              .putObject("index")
              .put("_index", "test")
              .put("_type", "_doc")
              .put("_id", Integer.toString(i+1))
              .put("_version", "1")
              .put("result", "created")
              .put("status", 201)
              .put("_seq_no", 0);
    }
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(response);
  }

  public static String errorBulkResponse(int items, String errorType, int... errorIdx) throws JsonProcessingException {
    ObjectNode response = MAPPER.createObjectNode();
    ArrayNode itemsArray = response
            .put("errors", true)
            .putArray("items");

    Set<Integer> errorIndexes = IntStream.of(errorIdx).boxed().collect(toSet());
    for (int i = 0; i < items; i++) {
      ObjectNode arrayObject = itemsArray
              .addObject()
              .putObject("index")
                .put("_index", "test")
                .put("_type", "_doc")
                .put("_id", Integer.toString(i + 1))
                .put("_version", "1")
                .put("_seq_no", 0);
      if (errorIndexes.contains(i)) {
        arrayObject
                .put("status", 400)
                .putObject("error")
                  .put("type", errorType)
                  .put("reason", "Reason for " + errorType);
      } else {
        arrayObject
                .put("result", "created")
                .put("status", 201);
      }
    }
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(response);
  }

}
