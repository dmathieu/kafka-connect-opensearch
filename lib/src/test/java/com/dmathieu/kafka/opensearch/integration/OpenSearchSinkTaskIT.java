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

package com.dmathieu.kafka.opensearch.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnector;
import com.dmathieu.kafka.opensearch.OpenSearchSinkTask;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.StringConverter;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.github.tomakehurst.wiremock.WireMockServer;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.*;
import static com.dmathieu.kafka.opensearch.integration.OpenSearchConnectorNetworkIT.errorBulkResponse;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class OpenSearchSinkTaskIT {

  protected static final String TOPIC = "test";
  protected static final int TASKS_MAX = 1;
  protected WireMockServer wireMockServer;

  @BeforeEach
  public void setup() {
    wireMockServer = new WireMockServer(options().dynamicPort());
    wireMockServer.start();
    wireMockServer.stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));
  }

  @AfterEach
  public void cleanup() {
    wireMockServer.stop();
  }

  @Test
  public void testOffsetCommit() {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(ok().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    OpenSearchSinkTask task = new OpenSearchSinkTask();
    TopicPartition tp = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1));
    task.put(records);

    // Nothing should be committed at this point
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(2));
    assertThat(task.preCommit(currentOffsets)).isEmpty();
  }

  @Test
  public void testIndividualFailure() throws JsonProcessingException {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(OpenSearchConnectorNetworkIT.errorBulkResponse(3,
                    "strict_dynamic_mapping_exception", 1))));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "3");
    props.put(LINGER_MS_CONFIG, "10000");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "ignore");

    final OpenSearchSinkTask task = new OpenSearchSinkTask();
    TopicPartition tp = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = IntStream.range(0, 6).boxed()
            .map(offset -> sinkRecord(tp, offset))
            .collect(toList());
    task.put(records);

    // All is safe to commit
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(6));
    assertThat(task.preCommit(currentOffsets))
            .isEqualTo(currentOffsets);

    // Now check that we actually fail and offsets are not past the failed record
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "fail");
    task.initialize(context);
    task.start(props);

    assertThatThrownBy(() -> task.put(records))
            .isInstanceOf(ConnectException.class)
            .hasMessageContaining("Indexing record failed");

    currentOffsets = ImmutableMap.of(tp, new OffsetAndMetadata(0));
    assertThat(getOffsetOrZero(task.preCommit(currentOffsets), tp))
            .isLessThanOrEqualTo(1);
  }

  @Test
  public void testConvertDataException() throws JsonProcessingException {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse(3))));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "10000");
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(DROP_INVALID_MESSAGE_CONFIG, "true");

    final OpenSearchSinkTask task = new OpenSearchSinkTask();
    TopicPartition tp = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1, null, "value"), // this should throw a DataException
            sinkRecord(tp, 2));
    task.put(records);

    // All is safe to commit
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(3));
    assertThat(task.preCommit(currentOffsets))
            .isEqualTo(currentOffsets);

    // Now check that we actually fail and offsets are not past the failed record
    props.put(DROP_INVALID_MESSAGE_CONFIG, "false");
    task.initialize(context);
    task.start(props);

    assertThatThrownBy(() -> task.put(records))
            .isInstanceOf(DataException.class)
            .hasMessageContaining("Key is used as document id and can not be null");

    currentOffsets = ImmutableMap.of(tp, new OffsetAndMetadata(0));
    assertThat(task.preCommit(currentOffsets).get(tp).offset())
            .isLessThanOrEqualTo(1);
  }

  @Test
  public void testNullValue() throws JsonProcessingException {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse(3))));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "10000");
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, "ignore");

    final OpenSearchSinkTask task = new OpenSearchSinkTask();
    TopicPartition tp = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1, "testKey", null),
            sinkRecord(tp, 2));
    task.put(records);

    // All is safe to commit
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(3));
    assertThat(task.preCommit(currentOffsets))
            .isEqualTo(currentOffsets);

    // Now check that we actually fail and offsets are not past the failed record
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, "fail");
    task.initialize(context);
    task.start(props);

    assertThatThrownBy(() -> task.put(records))
            .isInstanceOf(DataException.class)
            .hasMessageContaining("null value encountered");

    currentOffsets = ImmutableMap.of(tp, new OffsetAndMetadata(0));
    assertThat(task.preCommit(currentOffsets).get(tp).offset())
            .isLessThanOrEqualTo(1);
  }

  /**
   * Verify things are handled correctly when we receive the same records in a new put call
   * (e.g. after a RetriableException)
   */
  @Test
  public void testPutRetry() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(okJson(OpenSearchConnectorNetworkIT.errorBulkResponse())));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":1}"))
            .willReturn(aResponse().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    OpenSearchSinkTask task = new OpenSearchSinkTask();
    TopicPartition tp = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1));
    task.open(ImmutableList.of(tp));
    task.put(records);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(2));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(OpenSearchConnectorNetworkIT.errorBulkResponse())));

    task.put(records);
    await().untilAsserted(() ->
            assertThat(task.preCommit(currentOffsets))
                    .isEqualTo(currentOffsets));
  }

  /**
   * Verify partitions are paused and resumed
   */
  /*@Test @Disabled
  public void testOffsetsBackpressure() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(OpenSearchConnectorNetworkIT.errorBulkResponse())));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(okJson(OpenSearchConnectorNetworkIT.errorBulkResponse())
                    .withTransformers(BlockingTransformer.NAME)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "2");

    OpenSearchSinkTask task = new OpenSearchSinkTask();

    TopicPartition tp1 = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp1));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      records.add(sinkRecord(tp1, i));
    }
    task.put(records);

    verify(context).pause(tp1);

    BlockingTransformer.getInstance(wireMockServer).release(1);

    await().untilAsserted(() -> {
      task.put(Collections.emptyList());
      verify(context).resume(tp1);
    });
  }*/

  /**
   * Verify offsets are updated when partitions are closed/open
   */
  @Test @Disabled("Reenable when/if we reenable async flushing")
  public void testRebalance() throws Exception {
    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(okJson(OpenSearchConnectorNetworkIT.errorBulkResponse())));

    wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":1}"))
            .willReturn(aResponse().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    OpenSearchSinkTask task = new OpenSearchSinkTask();

    SinkTaskContext context = mock(SinkTaskContext.class);
    task.initialize(context);
    task.start(props);

    TopicPartition tp1 = new TopicPartition(TOPIC, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC, 1);
    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp1, 0),
            sinkRecord(tp1, 1),
            sinkRecord(tp2, 0));
    task.put(records);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(
                    tp1, new OffsetAndMetadata(2),
                    tp2, new OffsetAndMetadata(1));
    await().untilAsserted(() ->
            assertThat(task.preCommit(currentOffsets))
                    .isEqualTo(ImmutableMap.of(tp1, new OffsetAndMetadata(1),
                            tp2, new OffsetAndMetadata(1))));

    task.close(ImmutableList.of(tp1));
    task.open(ImmutableList.of(new TopicPartition(TOPIC, 2)));
    await().untilAsserted(() ->
            assertThat(task.preCommit(currentOffsets))
                    .isEqualTo(ImmutableMap.of(tp2, new OffsetAndMetadata(1))));
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset) {
    return sinkRecord(tp.topic(), tp.partition(), offset);
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset, String key, Object value) {
    return sinkRecord(tp.topic(), tp.partition(), offset, key, value);
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset) {
    return sinkRecord(topic, partition, offset, "testKey", ImmutableMap.of("doc_num", offset));
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset, String key, Object value) {
    return new SinkRecord(topic,
            partition,
            null,
            key,
            null,
            value,
            offset);
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

    // connector specific
    props.put(CONNECTION_URL_CONFIG, wireMockServer.url("/"));
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.toString());

    return props;
  }

  private long getOffsetOrZero(Map<TopicPartition, OffsetAndMetadata> offsetMap, TopicPartition tp) {
    OffsetAndMetadata offsetAndMetadata = offsetMap.get(tp);
    return offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
  }
}
