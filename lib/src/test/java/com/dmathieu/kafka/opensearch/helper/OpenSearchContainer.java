/*
 * Copyright 2018 Confluent Inc.
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

package com.dmathieu.kafka.opensearch.helper;

import org.apache.kafka.common.config.SslConfigs;
import org.opensearch.client.security.user.User;
import org.opensearch.client.security.user.privileges.Role;
import org.opensearch.client.security.user.privileges.IndicesPrivileges;
import org.opensearch.client.security.user.privileges.Role.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;

import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SecurityProtocol;

import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig.SSL_CONFIG_PREFIX;

/**
 * A specialized TestContainer container for testing OpenSearch.
 */
public class OpenSearchContainer
    extends org.testcontainers.elasticsearch.ElasticsearchContainer {

  private static final Logger log = LoggerFactory.getLogger(OpenSearchContainer.class);

  /**
   * Default OpenSearch Docker image name.
   */
  public static final String DEFAULT_DOCKER_IMAGE_NAME =
      "opensearchproject/opensearch";

  /**
   * Default OpenSearch version.
   */
  public static final String DEFAULT_OS_VERSION = "1.1.0";

  /**
   * Default OpenSearch port.
   */
  public static final int OPENSEARCH_DEFAULT_PORT = 9200;

  /**
   * Path to the OpenSearch configuration directory.
   */
  public static String CONFIG_PATH = "/usr/share/opensearch/config";

  /**
   * Create an {@link OpenSearchContainer} using the image name specified in the
   * {@code opensearch.image} system property or {@code OPENSEARCH_IMAGE} environment
   * variable, or defaulting to {@link #DEFAULT_DOCKER_IMAGE_NAME}, and the version specified in
   * the {@code opensearch.version} system property, {@code OPENSEARCH_VERSION} environment
   * variable, or defaulting to {@link #DEFAULT_ES_VERSION}.
   *
   * @return the unstarted container; never null
   */
  public static OpenSearchContainer fromSystemProperties() {
    String imageName = getSystemOrEnvProperty(
        "opensearch.image",
        "OPENSEARCH_IMAGE",
        DEFAULT_DOCKER_IMAGE_NAME
    );
    String version = getSystemOrEnvProperty(
        "opensearch.version",
        "OPENSEARCH_VERSION",
        DEFAULT_OS_VERSION
    );
    return new OpenSearchContainer(imageName + ":" + version);
  }

  public static OpenSearchContainer withESVersion(String ESVersion) {
    String imageName = getSystemOrEnvProperty(
        "opensearch.image",
        "OPENSEARCH_IMAGE",
        DEFAULT_DOCKER_IMAGE_NAME
    );
    return new OpenSearchContainer(imageName + ":" + ESVersion);
  }

  // Super user that has superuser role. Should not be used by connector
  private static final String OPENSEARCH_SUPERUSER_NAME = "admin";
  private static final String OPENSEARCH_SUPERUSER_PASSWORD = "admin";

  public static final String OPENSEARCH_USER_NAME = "admin";
  public static final String OPENSEARCH_USER_PASSWORD = "admin";
  private static final String OS_SINK_CONNECTOR_ROLE = "os_sink_connector_role";

  private static final long TWO_GIGABYTES = 2L * 1024 * 1024 * 1024;

  private final String imageName;
  private String keytabPath;

  /**
   * Create an OpenSearch container with the given image name with version qualifier.
   *
   * @param imageName the image name
   */
  public OpenSearchContainer(String imageName) {
    super(DockerImageName.parse(imageName).asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch"));
    this.imageName = imageName;
    withSharedMemorySize(TWO_GIGABYTES);
    withLogConsumer(this::containerLog);
  }

  @Override
  public void start() {
    super.start();

    Map<String, String> props = new HashMap<>();
    props.put(CONNECTION_USERNAME_CONFIG, OPENSEARCH_SUPERUSER_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, OPENSEARCH_SUPERUSER_PASSWORD);
    props.put(CONNECTION_URL_CONFIG, this.getConnectionUrl(false));
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

    OpenSearchHelperClient helperClient = getHelperClient(props);
    //createUsersAndRoles(helperClient);
  }

  private void createUsersAndRoles(OpenSearchHelperClient helperClient ) {
    Map<User, String> users = getUsers();
    List<Role> roles = getRoles();

    try {
      for (Role role: roles) {
        helperClient.createRole(role);
      }
      for (Map.Entry<User,String> userToPassword: users.entrySet()) {
        helperClient.createUser(userToPassword);
      }
    } catch (IOException e) {
      throw new ContainerLaunchException("Container startup failed", e);
    }
  }

  public OpenSearchContainer withKerberosEnabled(String keytab) {
    enableKerberos(keytab);
    return this;
  }

  /**
   * Set whether the OpenSearch instance should use Kerberos.
   *
   * <p>This can only be called <em>before</em> the container is started.
   *
   * @param keytab non-null keytab path if Kerberos is enabled
   */
  public void enableKerberos(String keytab) {
    if (isCreated()) {
      throw new IllegalStateException(
          "enableKerberos can only be used before the container is created."
      );
    }
    keytabPath = keytab;
  }

  /**
   * Get whether the OpenSearch instance is configured to use Kerberos.
   *
   * @return true if Kerberos is enabled, or false otherwise
   */
  public boolean isKerberosEnabled() {
    return keytabPath != null;
  }

  private String getFullResourcePath(String resourceName) {
    if (isKerberosEnabled()) {
      return "/kerberos/" + resourceName;
    }
    return "/default/" + resourceName;
  }

  @Override
  protected void configure() {
    super.configure();

    waitingFor(
        Wait.forLogMessage(".*Node '.*' initialized.*", 1)
            .withStartupTimeout(Duration.ofMinutes(5))
    );

    ImageFromDockerfile image = new ImageFromDockerfile()
        .withFileFromClasspath("opensearch.yml", getFullResourcePath("opensearch.yml"))
        .withFileFromClasspath("instances.yml", getFullResourcePath("instances.yml"))
        .withDockerfileFromBuilder(this::buildImage);

    if (isKerberosEnabled()) {
      log.info("Creating Kerberized OpenSearch image.");
      image.withFileFromFile("es.keytab", new File(keytabPath));
    } else {
      log.info("Using basic authentication");
      withEnv("OPENSEARCH_USERNAME", OPENSEARCH_SUPERUSER_NAME);
      withEnv("OPENSEARCH_PASSWORD", OPENSEARCH_SUPERUSER_PASSWORD);
    }

    log.info("Extending Docker image to generate certs and enable SSL");
    withEnv("OPENSEARCH_PASSWORD", OPENSEARCH_SUPERUSER_PASSWORD);
    withEnv("IP_ADDRESS", hostMachineIpAddress());

    image
      // Copy the script to generate the certs and start OpenSearch
      .withFileFromClasspath("start-opensearch.sh",
          getFullResourcePath("start-opensearch.sh"));
    setImage(image);
  }

  private void buildImage(DockerfileBuilder builder) {
    builder
      .from(imageName)
      .copy("opensearch.yml", CONFIG_PATH + "/opensearch.yml");

    log.info("Building OpenSearch image with SSL configuration");
    builder
      .copy("instances.yml", CONFIG_PATH + "/instances.yml")
      .copy("start-opensearch.sh", CONFIG_PATH + "/start-opensearch.sh")

      .user("root")
      .run("chown opensearch:opensearch " + CONFIG_PATH + "/opensearch.yml")
      .run("chown opensearch:opensearch " + CONFIG_PATH + "/instances.yml")
      .run("chown opensearch:opensearch " + CONFIG_PATH + "/start-opensearch.sh")
      .user("opensearch")

      .entryPoint(CONFIG_PATH + "/start-opensearch.sh");

    if (isKerberosEnabled()) {
      log.info("Building OpenSearch image with Kerberos configuration.");
      builder.copy("es.keytab", CONFIG_PATH + "/es.keytab");
    }
  }

  public String hostMachineIpAddress() {
    String dockerHost = System.getenv("DOCKER_HOST");
    if (dockerHost != null && !dockerHost.trim().isEmpty()) {
      try {
        URI url = new URI(dockerHost);
        dockerHost = url.getHost();
        log.info("Including DOCKER_HOST address {} in OpenSearch certs", dockerHost);
        return dockerHost;
      } catch (URISyntaxException e) {
        log.info("DOCKER_HOST={} could not be parsed into a URL: {}", dockerHost, e.getMessage(), e);
      }
    }
    try {
      String hostAddress = InetAddress.getLocalHost().getHostAddress();
      log.info("Including test machine address {} in OpenSearch certs", hostAddress);
      return hostAddress;
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * @see OpenSearchContainer#getConnectionUrl(boolean)
   */
  public String getConnectionUrl() {
    return getConnectionUrl(true);
  }

  /**
   * Get the OpenSearch connection URL.
   *
   * <p>This can only be called once the container is started.
   *
   * @param useContainerIpAddress use container IP if true, host machine's IP otherwise
   *
   * @return the connection URL; never null
   */
  public String getConnectionUrl(boolean useContainerIpAddress) {
    return String.format(
        "https://%s:%d",
        useContainerIpAddress ? getContainerIpAddress() : hostMachineIpAddress(),
        getMappedPort(OPENSEARCH_DEFAULT_PORT)
    );
  }

  protected String generateTemporaryFile(InputStream inputStream) throws IOException {
    File file = File.createTempFile("OpenSearchTestContainer", "");
    try (FileOutputStream outputStream = new FileOutputStream(file)) {
      IOUtils.copy(inputStream, outputStream);
    }
    return file.getAbsolutePath();
  }

  private static String getSystemOrEnvProperty(String sysPropName, String envPropName, String defaultValue) {
    String propertyValue = System.getProperty(sysPropName);
    if (null == propertyValue) {
      propertyValue = System.getenv(envPropName);
      if (null == propertyValue) {
        propertyValue = defaultValue;
      }
    }
    return propertyValue;
  }

  /**
   * Capture the container log by writing the container's standard output
   * to {@link System#out} (in yellow) and standard error to {@link System#err} (in red).
   *
   * @param logMessage the container log message
   */
  protected void containerLog(OutputFrame logMessage) {
    switch (logMessage.getType()) {
      case STDOUT:
        // Normal output in yellow
        System.out.print((char)27 + "[33m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      case STDERR:
        // Error output in red
        System.err.print((char)27 + "[31m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      case END:
        // End output in green
        System.err.print((char)27 + "[32m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      default:
        break;
    }
  }

  public OpenSearchHelperClient getHelperClient(Map<String, String> props) {
    // copy properties so that original properties are not affected
    Map<String, String> superUserProps = new HashMap<>(props);
    superUserProps.put(CONNECTION_USERNAME_CONFIG, OPENSEARCH_SUPERUSER_NAME);
    superUserProps.put(CONNECTION_PASSWORD_CONFIG, OPENSEARCH_SUPERUSER_PASSWORD);
    OpenSearchSinkConnectorConfig config = new OpenSearchSinkConnectorConfig(superUserProps);
    OpenSearchHelperClient client = new OpenSearchHelperClient(props.get(CONNECTION_URL_CONFIG), config);
    return client;
  }

  protected static List<Role> getRoles() {
    List<Role> roles = new ArrayList<>();
    roles.add(getMinimalPrivilegesRole());
    return roles;
  }

  protected static Map<User, String> getUsers() {
    Map<User, String> users = new HashMap<>();
    users.put(getMinimalPrivilegesUser(), getMinimalPrivilegesPassword());
    return users;
  }

  private static Role getMinimalPrivilegesRole() {
    IndicesPrivileges.Builder indicesPrivilegesBuilder = IndicesPrivileges.builder();
    IndicesPrivileges indicesPrivileges = indicesPrivilegesBuilder
        .indices("*")
        .privileges("create_index", "read", "write", "view_index_metadata")
        .build();
    Builder builder = Role.builder();
    builder = builder.clusterPrivileges("monitor");
    Role role = builder
        .name(OS_SINK_CONNECTOR_ROLE)
        .indicesPrivileges(indicesPrivileges)
        .build();
    return role;
  }

  private static User getMinimalPrivilegesUser() {
        return new User(OPENSEARCH_USER_NAME,
            Collections.singletonList(OS_SINK_CONNECTOR_ROLE));
  }

  private static String getMinimalPrivilegesPassword() {
    return OPENSEARCH_USER_PASSWORD;
  }
}
