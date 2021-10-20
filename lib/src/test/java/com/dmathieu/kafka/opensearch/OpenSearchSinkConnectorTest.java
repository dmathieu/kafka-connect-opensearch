package com.dmathieu.kafka.opensearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

public class OpenSearchSinkConnectorTest {

  private OpenSearchSinkConnector connector;
  private Map<String, String> settings;

  @BeforeEach
  public void before() {
    settings = OpenSearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    connector = new OpenSearchSinkConnector();
  }

  @Test
  public void shouldCatchInvalidConfigs() {
    assertThrows(ConnectException.class, () -> {
      connector.start(new HashMap<>());
    });
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    connector.start(settings);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
    assertFalse(taskConfigs.isEmpty(), "zero task configs provided");
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
    assertEquals(OpenSearchSinkTask.class, connector.taskClass());
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
