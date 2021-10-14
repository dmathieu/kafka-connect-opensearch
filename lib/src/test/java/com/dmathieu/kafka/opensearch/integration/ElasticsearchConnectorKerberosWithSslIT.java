package com.dmathieu.kafka.opensearch.integration;

import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;

import com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import com.dmathieu.kafka.opensearch.helper.ElasticsearchContainer;
import org.apache.kafka.common.config.SslConfigs;
import io.confluent.common.utils.IntegrationTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorKerberosWithSslIT extends ElasticsearchConnectorKerberosIT{

  @BeforeClass
  public static void setupBeforeAll() throws Exception {
    initKdc();

    container = ElasticsearchContainer
        .fromSystemProperties()
        .withKerberosEnabled(esKeytab)
        .withSslEnabled(true);
    container.start();
  }

  @Override
  @Test
  public void testKerberos() {
    // skip as parent is running this
    helperClient = null;
  }

  @Test
  public void testKerberosWithSsl() throws Exception {
    // Use IP address here because that's what the certificates allow
    String address = container.getConnectionUrl(false);

    props.put(CONNECTION_URL_CONFIG, address);
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, container.getKeystorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, container.getKeystorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, container.getTruststorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, container.getTruststorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, container.getKeyPassword());
    addKerberosConfigs(props);

    helperClient = container.getHelperClient(props);

    // Start connector
    runSimpleTest(props);
  }
}
