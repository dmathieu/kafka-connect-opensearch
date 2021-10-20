package com.dmathieu.kafka.opensearch.integration;

import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.KERBEROS_KEYTAB_PATH_CONFIG;
import static com.dmathieu.kafka.opensearch.ElasticsearchSinkConnectorConfig.KERBEROS_PRINCIPAL_CONFIG;

import com.dmathieu.kafka.opensearch.helper.OpenSearchContainer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.common.utils.IntegrationTest;

import org.junit.jupiter.api.*;

@Tag("Integration")
public class ElasticsearchConnectorKerberosIT extends ElasticsearchConnectorBaseIT {

  private static File baseDir;
  private static MiniKdc kdc;
  private static String esPrincipal;
  protected static String esKeytab;
  private static String userPrincipal;
  private static String userKeytab;

  @BeforeAll
  public static void setupBeforeAll() throws Exception {
    initKdc();

    container = OpenSearchContainer.fromSystemProperties().withKerberosEnabled(esKeytab);
    container.start();
  }

  /**
   * Shuts down the KDC and cleans up files.
   */
  @AfterAll
  public static void cleanupAfterAll() {
    container.close();
    closeKdc();
  }

  @Test
  public void testKerberos() throws Exception {

    addKerberosConfigs(props);
    helperClient = container.getHelperClient(props);
    runSimpleTest(props);
  }

  protected static void initKdc() throws Exception {
    baseDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
    if (baseDir.exists()) {
      deleteDirectory(baseDir.toPath());
    }

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    String es = "es";
    File keytabFile = new File(baseDir, es + ".keytab");
    esKeytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, es + "/localhost", "HTTP/localhost");
    esPrincipal = es + "/localhost@" + kdc.getRealm();

    String user = "connect-es";
    keytabFile = new File(baseDir, user + ".keytab");
    userKeytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, user + "/localhost");
    userPrincipal = user + "/localhost@" + kdc.getRealm();
  }

  protected static void addKerberosConfigs(Map<String, String> props) {
    props.put(KERBEROS_PRINCIPAL_CONFIG, userPrincipal);
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, userKeytab);
  }

  private static void closeKdc() {
    if (kdc != null) {
      kdc.stop();
    }

    if (baseDir.exists()) {
      deleteDirectory(baseDir.toPath());
    }
  }

  private static void deleteDirectory(Path directoryPath) {
    try {
      Files.walk(directoryPath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
