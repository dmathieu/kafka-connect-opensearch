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

import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BULK_SIZE_BYTES_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BehaviorOnNullValues;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.WriteMethod;
import com.dmathieu.kafka.opensearch.helper.OpenSearchContainer;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.storage.StringConverter;
import io.confluent.common.utils.IntegrationTest;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.search.SearchHit;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("Integration")
public class OpenSearchConnectorIT extends OpenSearchConnectorBaseIT {

  // TODO: test compatibility

  @BeforeAll
  public static void setupBeforeAll() {
    container = OpenSearchContainer.fromSystemProperties();
    container.start();
  }

  @Override @BeforeEach
  public void setup() {
    if (!container.isRunning()) {
      setupBeforeAll();
    }
    super.setup();
  }

  /**
   * Verify that mapping errors when an index has strict mapping is handled correctly
   */
  @Test
  public void testStrictMappings() throws Exception {
    helperClient.createIndex(TOPIC, "{ \"dynamic\" : \"strict\", " +
            " \"properties\": { \"longProp\": { \"type\": \"long\" } } } }");

    props.put(OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG, "1");
    props.put(OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG, "1");
    props.put(OpenSearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    connect.kafka().produce(TOPIC, "key1", "{\"longProp\":1}");
    connect.kafka().produce(TOPIC, "key2", "{\"any-prop\":1}");
    connect.kafka().produce(TOPIC, "key3", "{\"any-prop\":1}");
    connect.kafka().produce(TOPIC, "key4", "{\"any-prop\":1}");

    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("ElasticsearchException[Elasticsearch exception " +
                    "[type=strict_dynamic_mapping_exception," +
                    " reason=mapping set to strict, dynamic introduction of");

    // The framework commits offsets right before failing the task, verify the failed record's
    // offset is not included
    assertThat(getConnectorOffset(CONNECTOR_NAME, TOPIC, 0)).isEqualTo(1);
  }

  private long getConnectorOffset(String connectorName, String topic, int partition) throws Exception {
    String cGroupName = "connect-" + connectorName;
    ListConsumerGroupOffsetsResult offsetsResult = connect.kafka().createAdminClient()
            .listConsumerGroupOffsets(cGroupName);
    OffsetAndMetadata offsetAndMetadata = offsetsResult.partitionsToOffsetAndMetadata().get()
            .get(new TopicPartition(topic, partition));
    return offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
  }

  @Test
  public void testBatchByByteSize() throws Exception {
    // Based on the size of the topic, key, and value strings in JSON format.
    int approximateRecordByteSize = 60;
    props.put(BULK_SIZE_BYTES_CONFIG, Integer.toString(approximateRecordByteSize * 2));
    props.put(LINGER_MS_CONFIG, "180000");

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    writeRecords(3);
    // Only 2 records fit in 1 batch. The other record is sent once another record is written.
    verifySearchResults(2);

    writeRecords(1);
    verifySearchResults(4);
  }

  @Test
  public void testStopESContainer() throws Exception {
    props.put(OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG, "2");
    props.put(OpenSearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG, "1");
    props.put(OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG,
            Integer.toString(NUM_RECORDS - 1));

    // run connector and write
    runSimpleTest(props);

    // stop ES, for all following requests to fail with "connection refused"
    container.stop();

    // try to write some more
    writeRecords(NUM_RECORDS);

    // Connector should fail since the server is down
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
        assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
            .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
                    .contains("'java.net.ConnectException: Connection refused' after 3 attempt(s)");
  }

  @Test
  public void testChangeConfigsAndRestart() throws Exception {
    // run connector and write
    runSimpleTest(props);

    // restart
    props.put(BATCH_SIZE_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "1000");
    connect.configureConnector(CONNECTOR_NAME, props);

    // write some more
    writeRecords(NUM_RECORDS);
    verifySearchResults(NUM_RECORDS * 2);
  }

  @Test
  public void testDelete() throws Exception {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    runSimpleTest(props);

    // should have 5 records at this point
    // try deleting last one
    int lastRecord = NUM_RECORDS - 1;
    connect.kafka().produce(TOPIC, String.valueOf(lastRecord), null);

    // should have one less records
    verifySearchResults(NUM_RECORDS - 1);
  }

  @Test
  public void testHappyPath() throws Exception {
    runSimpleTest(props);
  }

  @Test @Disabled
  public void testHappyPathDataStream() throws Exception {
    setDataStream();

    runSimpleTest(props);

    assertEquals(index, helperClient.getDataStream(index).getName());
  }

  @Test
  public void testNullValue() throws Exception {
    runSimpleTest(props);

    // should have 5 records at this point
    // try writing null value
    connect.kafka().produce(TOPIC, String.valueOf(NUM_RECORDS), null);

    // should still have 5 records
    verifySearchResults(NUM_RECORDS);
  }

  /*
   * Currently writing primitives to ES fails because ES expects a JSON document and the connector
   * does not wrap primitives in any way into a JSON document.
   */
  @Test
  public void testPrimitive() throws Exception {
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    for (int i  = 0; i < NUM_RECORDS; i++) {
      connect.kafka().produce(TOPIC, String.valueOf(i),  String.valueOf(i));
    }

    waitForRecords(0);
  }

  @Test
  public void testUpsert() throws Exception {
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.toString());
    props.put(IGNORE_KEY_CONFIG, "false");
    runSimpleTest(props);

    // should have 10 records at this point
    // try updating last one
    int lastRecord = NUM_RECORDS - 1;
    connect.kafka().produce(TOPIC, String.valueOf(lastRecord), String.format("{\"doc_num\":%d}", 0));
    writeRecordsFromStartIndex(NUM_RECORDS, NUM_RECORDS);

    // should have double number of records
    verifySearchResults(NUM_RECORDS * 2);

    for (SearchHit hit : helperClient.search(TOPIC)) {
      if (Integer.parseInt(hit.getId()) == lastRecord) {
        // last record should be updated
        int docNum = (Integer) hit.getSourceAsMap().get("doc_num");
        assertEquals(0, docNum);
      }
    }
  }
}
