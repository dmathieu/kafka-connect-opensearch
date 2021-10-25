/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.dmathieu.kafka.opensearch;

import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BehaviorOnNullValues;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SecurityProtocol;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.WriteMethod;
import com.dmathieu.kafka.opensearch.helper.OpenSearchContainer;
import com.dmathieu.kafka.opensearch.helper.OpenSearchHelperClient;
import com.dmathieu.kafka.opensearch.helper.NetworkErrorContainer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.test.TestUtils;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.search.SearchHit;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class OpenSearchClientTest {

  private static final String INDEX = "index";
  private static final String OPENSEARCH_SUPERUSER_NAME = "admin";
  private static final String OPENSEARCH_SUPERUSER_PASSWORD = "admin";
  private static final String TOPIC = "index";
  private static final String DATA_STREAM_TYPE = "logs";
  private static final String DATA_STREAM_DATASET = "dataset";

  private static OpenSearchContainer container;

  private DataConverter converter;
  private OpenSearchHelperClient helperClient;
  private OpenSearchSinkConnectorConfig config;
  private Map<String, String> props;
  private String index;

  @BeforeAll
  public static void setupBeforeAll() {
    container = OpenSearchContainer.fromSystemProperties();
    container.start();
  }

  @AfterAll
  public static void cleanupAfterAll() {
    container.close();
  }

  @BeforeEach
  public void setup() {
    index = TOPIC;
    props = OpenSearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(LINGER_MS_CONFIG, "1000");

    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    helperClient = new OpenSearchHelperClient(container.getConnectionUrl(), config);
  }

  @AfterEach
  public void cleanup() throws IOException {
    if (helperClient != null && helperClient.indexExists(index)){
      helperClient.deleteIndex(index, config.isDataStream());
    }
  }

  @Test
  public void testClose() {
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.close();
  }

  @Test
  public void testCloseFails() throws Exception {
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    OpenSearchClient client = new OpenSearchClient(config, null) {
      @Override
      public void close() {
        try {
          if (!bulkProcessor.awaitClose(1, TimeUnit.MILLISECONDS)) {
            throw new ConnectException("Failed to process all outstanding requests in time.");
          }
        } catch (InterruptedException e) {}
      }
    };

    writeRecord(sinkRecord(0), client);
    assertThrows(ConnectException.class, () -> {
      client.close();
    });
    waitUntilRecordsInES(1);
  }

  @Test
  public void testCreateIndex() throws IOException {
    OpenSearchClient client = new OpenSearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    client.createIndexOrDataStream(index);
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test @Disabled
  public void testCreateExistingDataStream() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new OpenSearchSinkConnectorConfig(props);
    index = createIndexName(TOPIC);
    OpenSearchClient client = new OpenSearchClient(config, null);
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    assertFalse(client.createIndexOrDataStream(index));
    client.close();
  }

  @Test @Disabled
  public void testCreateNewDataStream() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new OpenSearchSinkConnectorConfig(props);
    index = createIndexName(TOPIC);
    OpenSearchClient client = new OpenSearchClient(config, null);
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testDoesNotCreateAlreadyExistingIndex() throws IOException {
    OpenSearchClient client = new OpenSearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));

    assertFalse(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexExists() throws IOException {
    OpenSearchClient client = new OpenSearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(client.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexDoesNotExist() throws IOException {
    OpenSearchClient client = new OpenSearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    assertFalse(client.indexExists(index));
    client.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateMapping() throws IOException {
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));

    Map<String, Object> mapping = helperClient.getMapping(index).sourceAsMap();
    assertTrue(mapping.containsKey("properties"));
    Map<String, Object> props = (Map<String, Object>) mapping.get("properties");
    assertTrue(props.containsKey("offset"));
    assertTrue(props.containsKey("another"));
    Map<String, Object> offset = (Map<String, Object>) props.get("offset");
    assertEquals("integer", offset.get("type"));
    assertEquals(0, offset.get("null_value"));
    Map<String, Object> another = (Map<String, Object>) props.get("another");
    assertEquals("integer", another.get("type"));
    assertEquals(0, another.get("null_value"));
    client.close();
  }

  @Test
  public void testHasMapping() {
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testDoesNotHaveMapping() {
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    assertFalse(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testBuffersCorrectly() throws Exception {
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    config = new OpenSearchSinkConnectorConfig(props);
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    assertEquals(1, client.numBufferedRecords.get());
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(INDEX));
    assertEquals(0, client.numBufferedRecords.get());

    writeRecord(sinkRecord(1), client);
    assertEquals(1, client.numBufferedRecords.get());

    // will block until the previous record is flushed
    writeRecord(sinkRecord(2), client);
    assertEquals(1, client.numBufferedRecords.get());

    waitUntilRecordsInES(3);
    client.close();
  }

  @Test
  public void testFlush() throws Exception {
    props.put(LINGER_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(1)));
    config = new OpenSearchSinkConnectorConfig(props);
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    assertEquals(0, helperClient.getDocCount(index)); // should be empty before flush

    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  @Test
  public void testIndexRecord() throws Exception {
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  @Test
  public void testDeleteRecord() throws Exception {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord("key0", 0), client);
    writeRecord(sinkRecord("key1", 1), client);
    client.flush();

    waitUntilRecordsInES(2);

    // delete 1
    SinkRecord deleteRecord = sinkRecord("key0", null, null, 3);
    writeRecord(deleteRecord, client);

    waitUntilRecordsInES(1);
    client.close();
  }

  @Test
  public void testUpsertRecords() throws Exception {
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord("key0", 0), client);
    writeRecord(sinkRecord("key1", 1), client);
    client.flush();

    waitUntilRecordsInES(2);

    // create modified record for upsert
    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.int32().defaultValue(0).build())
        .field("another", SchemaBuilder.int32().defaultValue(0).build())
        .build();

    Struct value = new Struct(schema).put("offset", 2);
    SinkRecord upsertRecord = sinkRecord("key0", schema, value, 2);
    Struct value2 = new Struct(schema).put("offset", 3);
    SinkRecord upsertRecord2 = sinkRecord("key0", schema, value2, 3);

    // upsert 2, write another
    writeRecord(upsertRecord, client);
    writeRecord(upsertRecord2, client);
    writeRecord(sinkRecord("key2", 4), client);
    client.flush();

    waitUntilRecordsInES(3);
    for (SearchHit hit : helperClient.search(index)) {
      if (hit.getId().equals("key0")) {
        assertEquals(3, hit.getSourceAsMap().get("offset"));
        assertEquals(0, hit.getSourceAsMap().get("another"));
      }
    }

    client.close();
  }

  @Test
  public void testIgnoreBadRecord() throws Exception {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("not_mapped_field", SchemaBuilder.int32().defaultValue(0).build())
        .build();
    Struct value = new Struct(schema).put("not_mapped_field", 420);
    SinkRecord badRecord = sinkRecord("key", schema, value, 0);

    writeRecord(sinkRecord(0), client);
    client.flush();

    writeRecord(badRecord, client);
    client.flush();

    writeRecord(sinkRecord(1), client);
    client.flush();

    waitUntilRecordsInES(2);
    assertEquals(2, helperClient.getDocCount(index));
    client.close();
  }

  @Test
  public void testFailOnBadRecord() throws Exception {
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = sinkRecord("key", schema, value, 0);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    writeRecord(badRecord, client);
    client.flush();

    assertThrows(ConnectException.class, () -> {
      // consecutive index calls should cause exception
      try {
        for (int i = 0; i < 5; i++) {
          writeRecord(sinkRecord(i + 1), client);
          client.flush();
          waitUntilRecordsInES(i + 2);
        }
      } catch (ConnectException e) {
        client.close();
        throw e;
      }
    });
  }

  @Test
  public void testRetryRecordsOnSocketTimeoutFailure() throws Exception {
    props.put(LINGER_MS_CONFIG, "60000");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_RETRIES_CONFIG, "100");
    props.put(RETRY_BACKOFF_MS_CONFIG, "1000");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    // mock bulk processor to throw errors
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.createIndexOrDataStream(index);

    // bring down ES service
    NetworkErrorContainer delay = new NetworkErrorContainer(container.getContainerName());
    delay.start();

    // attempt a write
    writeRecord(sinkRecord(0), client);
    client.flush();

    // keep the ES service down for a couple of timeouts
    Thread.sleep(config.readTimeoutMs() * 4L);

    // bring up ES service
    delay.stop();

    waitUntilRecordsInES(1);
  }

  @Test
  public void testReporter() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
    OpenSearchClient client = new OpenSearchClient(config, reporter);
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = sinkRecord("key0", schema, value, 1);

    writeRecord(sinkRecord("key0", 0), client);
    client.flush();
    waitUntilRecordsInES(1);

    writeRecord(badRecord, client);
    client.flush();

    // failed requests take a bit longer
    for (int i = 2; i < 7; i++) {
      writeRecord(sinkRecord("key" + i, i + 1), client);
      client.flush();
      waitUntilRecordsInES(i);
    }

    verify(reporter, times(1)).report(eq(badRecord), any(Throwable.class));
    client.close();
  }

  @Test
  public void testReporterNotCalled() throws Exception {
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    OpenSearchClient client = new OpenSearchClient(config, reporter);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    writeRecord(sinkRecord(1), client);
    writeRecord(sinkRecord(2), client);
    client.flush();

    waitUntilRecordsInES(3);
    assertEquals(3, helperClient.getDocCount(index));
    verify(reporter, never()).report(eq(sinkRecord(0)), any(Throwable.class));
    client.close();
  }

  @Test
  public void testNoVersionConflict() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ErrantRecordReporter reporter2 = mock(ErrantRecordReporter.class);
    OpenSearchClient client = new OpenSearchClient(config, reporter);
    OpenSearchClient client2 = new OpenSearchClient(config, reporter2);

    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    writeRecord(sinkRecord(1), client2);
    writeRecord(sinkRecord(2), client);
    writeRecord(sinkRecord(3), client2);
    writeRecord(sinkRecord(4), client);
    writeRecord(sinkRecord(5), client2);

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    verify(reporter, never()).report(any(SinkRecord.class), any(Throwable.class));
    verify(reporter2, never()).report(any(SinkRecord.class), any(Throwable.class));
    client.close();
    client2.close();
  }

  @Test @Disabled
  public void testWriteDataStreamInjectTimestamp() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new OpenSearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    OpenSearchClient client = new OpenSearchClient(config, null);
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));

    // Sink Record does not include the @timestamp field in its value.
    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  private String createIndexName(String name) {
    return config.isDataStream()
        ? String.format("%s-%s-%s", DATA_STREAM_TYPE, DATA_STREAM_DATASET, name)
        : name;
  }

  @Test
  public void testConnectionUrlExtraSlash() {
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl() + "/");
    config = new OpenSearchSinkConnectorConfig(props);
    OpenSearchClient client = new OpenSearchClient(config, null);
    client.close();
  }

  private static Schema schema() {
    return SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.int32().defaultValue(0).build())
        .field("another", SchemaBuilder.int32().defaultValue(0).build())
        .build();
  }

  private static SinkRecord sinkRecord(int offset) {
    return sinkRecord("key", offset);
  }

  private static SinkRecord sinkRecord(String key, int offset) {
    Struct value = new Struct(schema()).put("offset", offset).put("another", offset + 1);
    return sinkRecord(key, schema(), value, offset);
  }

  private static SinkRecord sinkRecord(String key, Schema schema, Struct value, int offset) {
    return new SinkRecord(
        TOPIC,
        0,
        Schema.STRING_SCHEMA,
        key,
        schema,
        value,
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );
  }

  private void waitUntilRecordsInES(int expectedRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(index) == expectedRecords;
          } catch (OpenSearchStatusException e) {
            if (e.getMessage().contains("index_not_found_exception")) {
              return false;
            }

            throw e;
          }
        },
        TimeUnit.MINUTES.toMillis(1),
        String.format("Could not find expected documents (%d) in time.", expectedRecords)
    );
  }

  private void writeRecord(SinkRecord record, OpenSearchClient client) {
    client.index(record, converter.convertRecord(record, createIndexName(record.topic())),
            new OffsetTracker.OffsetState(record.kafkaOffset()));
  }
}
