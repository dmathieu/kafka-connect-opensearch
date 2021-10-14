package com.dmathieu.kafka.opensearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Before;
import org.junit.Test;

public class ElasticsearchSinkConnectorTest {

  private ElasticsearchSinkConnector connector;
  private Map<String, String> settings;

  @Before
  public void before() {
    settings = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    connector = new ElasticsearchSinkConnector();
  }

  @Test(expected = ConnectException.class)
  public void shouldCatchInvalidConfigs() {
    connector.start(new HashMap<>());
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    connector.start(settings);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
    assertFalse("zero task configs provided", taskConfigs.isEmpty());
    for (Map<String, String> taskConfig : taskConfigs) {
      assertEquals(settings, taskConfig);
    }
  }

  @Test
  public void shouldNotHaveNullConfigDef() {
    // ConfigDef objects don't have an overridden equals() method; just make sure it's non-null
    assertNotNull(connector.config());
  }

  @Test
  public void shouldReturnConnectorType() {
    assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
  }

  @Test
  public void shouldReturnSinkTask() {
    assertEquals(ElasticsearchSinkTask.class, connector.taskClass());
  }

  @Test
  public void shouldStartAndStop() {
    connector.start(settings);
    connector.stop();
  }

  @Test
  public void testVersion() {
    assertNotNull(connector.version());
    assertFalse(connector.version().equals("0.0.0.0"));
    assertFalse(connector.version().equals("unknown"));
    // Match semver with potentially a qualifier in the end
    assertTrue(connector.version().matches("^(\\d+\\.){2}?(\\*|\\d+)(-.*)?$"));
  }
}
