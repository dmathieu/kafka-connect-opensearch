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

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.Builder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dmathieu.kafka.opensearch.OpenSearchSinkConnector;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig;
import com.dmathieu.kafka.opensearch.helper.OpenSearchContainer;
import com.dmathieu.kafka.opensearch.helper.OpenSearchHelperClient;

import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import org.apache.kafka.common.config.SslConfigs;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class OpenSearchConnectorBaseIT extends BaseConnectorIT {

  protected static final int NUM_RECORDS = 5;
  protected static final int TASKS_MAX = 1;
  protected static final String CONNECTOR_NAME = "es-connector";
  protected static final String TOPIC = "test";

  protected static OpenSearchContainer container;

  protected boolean isDataStream;
  protected OpenSearchHelperClient helperClient;
  protected Map<String, String> props;
  protected String index;

  @AfterAll
  public static void cleanupAfterAll() {
    container.close();
  }

  @BeforeEach
  public void setup() {
    index = TOPIC;
    isDataStream = false;

    startConnect();
    connect.kafka().createTopic(TOPIC);

    props = createProps();
    helperClient = container.getHelperClient(props);
  }

  @AfterEach
  public void cleanup() throws IOException {
    stopConnect();

    if (container.isRunning()) {
      if (helperClient != null) {
        try {
          helperClient.deleteIndex(index, isDataStream);
          helperClient.close();
        } catch (ConnectException e) {
          // Server is already down. No need to close
        }
      }
    }
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
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");

    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    props.put(CONNECTION_USERNAME_CONFIG, "admin");
    props.put(CONNECTION_PASSWORD_CONFIG, "admin");

    return props;
  }

  protected void runSimpleTest(Map<String, String> props) throws Exception {
    // start the connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    writeRecords(NUM_RECORDS);

    verifySearchResults(NUM_RECORDS);
  }

  protected void setDataStream() {
    isDataStream = true;
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    props.put(DATA_STREAM_DATASET_CONFIG, "dataset");
    index = "logs-dataset-" + TOPIC;
  }

  protected void setupFromContainer() {
    String address = container.getConnectionUrl();
    props.put(CONNECTION_URL_CONFIG, address);
    helperClient = new OpenSearchHelperClient(
        props.get(CONNECTION_URL_CONFIG),
        new OpenSearchSinkConnectorConfig(props)
    );
  }

  protected void verifySearchResults(int numRecords) throws Exception {
    waitForRecords(numRecords);

    for (SearchHit hit : helperClient.search(index)) {
      int id = (Integer) hit.getSourceAsMap().get("doc_num");
      assertNotNull(id);
      assertTrue(id < numRecords);

      if (isDataStream) {
        assertTrue(hit.getIndex().contains(index));
      } else {
        assertEquals(index, hit.getIndex());
      }
    }
  }

  protected void waitForRecords(int numRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(index) == numRecords;
          } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("index_not_found_exception")) {
              return false;
            }

            throw e;
          }
        },
        CONSUME_MAX_DURATION_MS,
        "Sufficient amount of document were not found in ES on time."
    );
  }

  protected void writeRecords(int numRecords) {
    writeRecordsFromStartIndex(0, numRecords);
  }

  protected void writeRecordsFromStartIndex(int start, int numRecords) {
    for (int i  = start; i < start + numRecords; i++) {
      connect.kafka().produce(
          TOPIC,
          String.valueOf(i),
          String.format("{\"doc_num\":%d,\"@timestamp\":\"2021-04-28T11:11:22.%03dZ\"}", i, i)
      );
    }
  }
}
