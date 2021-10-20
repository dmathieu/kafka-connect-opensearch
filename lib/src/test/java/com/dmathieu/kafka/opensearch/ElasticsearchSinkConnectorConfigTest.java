package com.dmathieu.kafka.opensearch;


import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.KERBEROS_KEYTAB_PATH_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.PROXY_HOST_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.PROXY_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.PROXY_PORT_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.PROXY_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;

import static com.dmathieu.kafka.opensearch.helper.OpenSearchContainer.OPENSEARCH_USER_NAME;
import static com.dmathieu.kafka.opensearch.helper.OpenSearchContainer.OPENSEARCH_USER_PASSWORD;

import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;

import com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

public class ElasticsearchSinkConnectorConfigTest {

  private Map<String, String> props;

  @BeforeEach
  public void setup() {
    props = addNecessaryProps(new HashMap<>());
  }

  @Test
  public void testDefaultHttpTimeoutsConfig() {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(config.readTimeoutMs(), 3000);
    assertEquals(config.connectionTimeoutMs(), 1000);
  }

  @Test
  public void testSetHttpTimeoutsConfig() {
    props.put(READ_TIMEOUT_MS_CONFIG, "10000");
    props.put(CONNECTION_TIMEOUT_MS_CONFIG, "15000");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    assertEquals(config.readTimeoutMs(), 10000);
    assertEquals(config.connectionTimeoutMs(), 15000);
  }

  @Test
  public void shouldAllowValidChractersDataStreamDataset() {
    props.put(DATA_STREAM_DATASET_CONFIG, "a_valid.dataset123");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidDataStreamType() {
    props.put(DATA_STREAM_TYPE_CONFIG, "metrics");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidDataStreamTypeCaseInsensitive() {
    props.put(DATA_STREAM_TYPE_CONFIG, "mEtRICS");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldNotAllowInvalidCaseDataStreamDataset() {
    assertThrows(ConfigException.class, () -> {
      props.put(DATA_STREAM_DATASET_CONFIG, "AN_INVALID.dataset123");
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowInvalidCharactersDataStreamDataset() {
    assertThrows(ConfigException.class, () -> {
    props.put(DATA_STREAM_DATASET_CONFIG, "not-valid?");
    new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowInvalidDataStreamType() {
    assertThrows(ConfigException.class, () -> {
    props.put(DATA_STREAM_TYPE_CONFIG, "notLogOrMetrics");
    new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowLongDataStreamDataset() {
    assertThrows(ConfigException.class, () -> {
    props.put(DATA_STREAM_DATASET_CONFIG, String.format("%d%100d", 1, 1));
    new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowNullUrlList(){
    assertThrows(ConfigException.class, () -> {
      props.put(CONNECTION_URL_CONFIG, null);
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void testSslConfigs() {
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "opensesame");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/path2");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "opensesame2");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    Map<String, Object> sslConfigs = config.sslConfigs();
    assertTrue(sslConfigs.size() > 0);
    assertEquals(
        new Password("opensesame"),
        sslConfigs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
    );
    assertEquals(
        new Password("opensesame2"),
        sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    );
    assertEquals("/path", sslConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("/path2", sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
  }

  @Test
  public void shouldAcceptValidBasicProxy() {
    props.put(PROXY_HOST_CONFIG, "proxy host");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    assertNotNull(config);
    assertTrue(config.isBasicProxyConfigured());
    assertFalse(config.isProxyWithAuthenticationConfigured());
  }

  @Test
  public void shouldAcceptValidProxyWithAuthentication() {
    props.put(PROXY_HOST_CONFIG, "proxy host");
    props.put(PROXY_PORT_CONFIG, "1010");
    props.put(PROXY_USERNAME_CONFIG, "username");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    assertNotNull(config);
    assertTrue(config.isBasicProxyConfigured());
    assertTrue(config.isProxyWithAuthenticationConfigured());
    assertEquals("proxy host", config.proxyHost());
    assertEquals(1010, config.proxyPort());
    assertEquals("username", config.proxyUsername());
    assertEquals("password", config.proxyPassword().value());
  }

  @Test
  public void shouldNotAllowInvalidProxyPort() {
    assertThrows(ConfigException.class, () -> {
      props.put(PROXY_PORT_CONFIG, "-666");
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowInvalidUrl() {
    assertThrows(ConfigException.class, () -> {
      props.put(CONNECTION_URL_CONFIG, ".com:/bbb/dfs,http://valid.com");
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowInvalidSecurityProtocol() {
    assertThrows(ConfigException.class, () -> {
      props.put(SECURITY_PROTOCOL_CONFIG, "unsecure");
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldDisableHostnameVerification() {
    props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertFalse(config.shouldDisableHostnameVerification());

    props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    config = new ElasticsearchSinkConnectorConfig(props);
    assertTrue(config.shouldDisableHostnameVerification());

    props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null);
    config = new ElasticsearchSinkConnectorConfig(props);
    assertFalse(config.shouldDisableHostnameVerification());
  }

  @Test
  public void shouldNotAllowInvalidExtensionKeytab() {
    assertThrows(ConfigException.class, () -> {
      props.put(KERBEROS_KEYTAB_PATH_CONFIG, "keytab.wrongextension");
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldNotAllowNonExistingKeytab() {
    assertThrows(ConfigException.class, () -> {
      props.put(KERBEROS_KEYTAB_PATH_CONFIG, "idontexist.keytab");
      new ElasticsearchSinkConnectorConfig(props);
    });
  }

  @Test
  public void shouldAllowValidKeytab() throws IOException {
    Path keytab = Files.createTempFile("iexist", ".keytab");
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());

    new ElasticsearchSinkConnectorConfig(props);

    keytab.toFile().delete();
  }

  public static Map<String, String> addNecessaryProps(Map<String, String> props) {
    if (props == null) {
      props = new HashMap<>();
    }
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost:8080");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    props.put(CONNECTION_USERNAME_CONFIG, OPENSEARCH_USER_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, OPENSEARCH_USER_PASSWORD);

    return props;
  }
}
