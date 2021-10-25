/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.dmathieu.kafka.opensearch;

import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_KEY_TOPICS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.IGNORE_SCHEMA_TOPICS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.KERBEROS_KEYTAB_PATH_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.PROXY_HOST_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.PROXY_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.PROXY_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.KERBEROS_PRINCIPAL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SecurityProtocol;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SslConfigs;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.MainResponse;
import org.opensearch.rest.RestStatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.Mockito;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

public class ValidatorTest {

  private MainResponse mockInfoResponse;
  private Map<String, String> props;
  private RestHighLevelClient mockClient;
  private Validator validator;

  @BeforeEach
  public void setup() throws IOException {
    props = OpenSearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());

    mockClient = mock(RestHighLevelClient.class);
    when(mockClient.ping(any(RequestOptions.class))).thenReturn(true);
    mockInfoResponse = mock(MainResponse.class, Mockito.RETURNS_DEEP_STUBS);
    when(mockClient.info(any(RequestOptions.class))).thenReturn(mockInfoResponse);
    when(mockInfoResponse.getVersion().getNumber()).thenReturn("1.1.0");
  }

  @Test
  public void testValidDefaultConfig() {
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidIndividualConfigs() {
    validator = new Validator(new HashMap<>(), () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Missing required configuration");
  }

  @Test
  public void testValidUpsertDeleteOnDefaultConfig() {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
    props.put(WRITE_METHOD_CONFIG, "upsert");
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidCredentials() {
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    props.remove(CONNECTION_PASSWORD_CONFIG);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_USERNAME_CONFIG, "must be set");
    assertHasErrorMessage(result, CONNECTION_PASSWORD_CONFIG, "must be set");
    props.remove(CONNECTION_USERNAME_CONFIG);

    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props);
    result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_USERNAME_CONFIG, "must be set");
    assertHasErrorMessage(result, CONNECTION_PASSWORD_CONFIG, "must be set");
  }

  @Test
  public void testClientThrowsOpenSearchStatusException() throws IOException {
    when(mockClient.ping(any(RequestOptions.class))).thenThrow(new OpenSearchStatusException("Deleted resource.", RestStatus.GONE));
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Could not connect to OpenSearch. Error message: Deleted resource.");
  }

  @Test
  public void testValidCredentials() {
    // username and password not set
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // both set
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidMissingOneDataStreamConfig() {
    props.put(DATA_STREAM_DATASET_CONFIG, "a_valid_dataset");
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, DATA_STREAM_DATASET_CONFIG, "must be set");
    assertHasErrorMessage(result, DATA_STREAM_TYPE_CONFIG, "must be set");
  }

  @Test
  public void testInvalidUpsertDeleteOnValidDataStreamConfigs() {
    props.put(DATA_STREAM_DATASET_CONFIG, "a_valid_dataset");
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);

    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
    props.put(WRITE_METHOD_CONFIG, "upsert");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, BEHAVIOR_ON_NULL_VALUES_CONFIG, "must not be");
    assertHasErrorMessage(result, WRITE_METHOD_CONFIG, "must not be");
  }

  @Test
  public void testInvalidIgnoreConfigs() {
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_KEY_TOPICS_CONFIG, "some,topics");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    props.put(IGNORE_SCHEMA_TOPICS_CONFIG, "some,other,topics");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, IGNORE_KEY_CONFIG, "is true");
    assertHasErrorMessage(result, IGNORE_KEY_TOPICS_CONFIG, "is true");
    assertHasErrorMessage(result, IGNORE_SCHEMA_CONFIG, "is true");
    assertHasErrorMessage(result, IGNORE_SCHEMA_TOPICS_CONFIG, "is true");
  }

  @Test
  public void testValidIgnoreConfigs() {
    // topics configs not set
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // ignore configs are false
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(IGNORE_KEY_TOPICS_CONFIG, "some,topics");
    props.put(IGNORE_SCHEMA_CONFIG, "false");
    props.put(IGNORE_SCHEMA_TOPICS_CONFIG, "some,other,topics");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidKerberos() throws IOException {
    props.remove(CONNECTION_USERNAME_CONFIG);
    props.remove(CONNECTION_PASSWORD_CONFIG);

    props.put(KERBEROS_PRINCIPAL_CONFIG, "principal");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, KERBEROS_PRINCIPAL_CONFIG, "must be set");
    assertHasErrorMessage(result, KERBEROS_KEYTAB_PATH_CONFIG, "must be set");

    // proxy
    Path keytab = Files.createTempFile("es", ".keytab");
    props.put(KERBEROS_PRINCIPAL_CONFIG, "principal");
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());
    props.put(PROXY_HOST_CONFIG, "proxy.com");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, KERBEROS_PRINCIPAL_CONFIG, "not supported with proxy settings");
    assertHasErrorMessage(result, KERBEROS_KEYTAB_PATH_CONFIG, "not supported with proxy settings");
    assertHasErrorMessage(result, PROXY_HOST_CONFIG, "not supported with proxy settings");

    // basic credentials
    props.remove(PROXY_HOST_CONFIG);
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, KERBEROS_PRINCIPAL_CONFIG, "Either only Kerberos");
    assertHasErrorMessage(result, KERBEROS_KEYTAB_PATH_CONFIG, "Either only Kerberos");
    assertHasErrorMessage(result, CONNECTION_USERNAME_CONFIG, "Either only Kerberos");
    assertHasErrorMessage(result, CONNECTION_PASSWORD_CONFIG, "Either only Kerberos");

    keytab.toFile().delete();
  }

  @Test
  public void testValidKerberos() throws IOException {
    props.remove(CONNECTION_USERNAME_CONFIG);
    props.remove(CONNECTION_PASSWORD_CONFIG);

    // kerberos configs not set
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // kerberos configs both false
    Path keytab = Files.createTempFile("es", ".keytab");
    props.put(KERBEROS_PRINCIPAL_CONFIG, "principal");
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
    keytab.toFile().delete();
  }

  @Test
  public void testInvalidLingerMs() {
    props.put(LINGER_MS_CONFIG, "1001");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "1000");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, LINGER_MS_CONFIG, "can not be larger than");
    assertHasErrorMessage(result, FLUSH_TIMEOUT_MS_CONFIG, "can not be larger than");
  }

  @Test
  public void testValidLingerMs() {
    props.put(LINGER_MS_CONFIG, "999");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "1000");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidMaxBufferedRecords() {
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, MAX_BUFFERED_RECORDS_CONFIG, "must be larger than or equal to");
    assertHasErrorMessage(result, BATCH_SIZE_CONFIG, "must be larger than or equal to");
    assertHasErrorMessage(result, MAX_IN_FLIGHT_REQUESTS_CONFIG, "must be larger than or equal to");
  }

  @Test
  public void testValidMaxBufferedRecords() {
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "5");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidProxy() {
    props.put(PROXY_HOST_CONFIG, "");
    props.put(PROXY_USERNAME_CONFIG, "username");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, PROXY_HOST_CONFIG, " must be set to use");
    assertHasErrorMessage(result, PROXY_USERNAME_CONFIG, " must be set to use");
    assertHasErrorMessage(result, PROXY_PASSWORD_CONFIG, " must be set to use");

    props.remove(PROXY_USERNAME_CONFIG);

    props.put(PROXY_HOST_CONFIG, "proxy");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, PROXY_USERNAME_CONFIG, "Either both or neither");
    assertHasErrorMessage(result, PROXY_PASSWORD_CONFIG, "Either both or neither");
  }

  @Test
  public void testValidProxy() {
    props.put(PROXY_HOST_CONFIG, "proxy");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    props.put(PROXY_HOST_CONFIG, "proxy");
    props.put(PROXY_USERNAME_CONFIG, "password");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testIncompatibleESVersionWithConnector() {
    validator = new Validator(props, () -> mockClient);
    when(mockInfoResponse.getVersion().getNumber()).thenReturn("0.0.5");
    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "not compatible with OpenSearch");
  }

  @Test
  public void testCompatibleESVersionWithConnector() {
    validator = new Validator(props, () -> mockClient);
    String[] compatibleOSVersions = {"1.0.0", "1.0.1", "1.1.0", "1.1.2"};
    for (String version : compatibleOSVersions) {
      when(mockInfoResponse.getVersion().getNumber()).thenReturn(version);
      Config result = validator.validate();

      assertNoErrors(result);
    }
  }

  @Test
  public void testValidConnection() {
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidConnection() throws IOException {
    when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenReturn(false);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Could not connect to OpenSearch.");
  }

  @Test
  public void testInvalidConnectionThrows() throws IOException {
    when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenThrow(new IOException("i iz fake"));
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Could not connect to OpenSearch.");
  }

  @Test
  public void testTimestampMappingDataStreamSet() {
    configureDataStream();
    props.put(DATA_STREAM_TIMESTAMP_CONFIG, "one, two, fields");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();

    assertNoErrors(result);
  }

  @Test
  public void testTimestampMappingDataStreamNotSet() {
    props.put(DATA_STREAM_TIMESTAMP_CONFIG, "one, two, fields");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();

    assertHasErrorMessage(result, DATA_STREAM_TIMESTAMP_CONFIG, "only necessary for data streams");
  }


  private static void assertHasErrorMessage(Config config, String property, String msg) {
    for (ConfigValue configValue : config.configValues()) {
      if (configValue.name().equals(property)) {
        assertFalse(configValue.errorMessages().isEmpty());
        assertTrue(configValue.errorMessages().get(0).contains(msg));
      }
    }
  }

  private static void assertNoErrors(Config config) {
    config.configValues().forEach(c -> assertTrue(c.errorMessages().isEmpty()));
  }

  private void configureDataStream() {
    props.put(DATA_STREAM_DATASET_CONFIG, "a_valid_dataset");
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
  }
}
