# Kafka Connect OpenSearch Connector

kafka-connect-opensearch is a fork of Confluent's [kafka-connect-elasticsearch](https://github.com/confluentinc/kafka-connect-elasticsearch).  
It allows runnig a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for copying data between Kafka and OpenSearch.

# Usage

## Installation

Each release publishes a JAR of the package which can be downloaded and used directly.

```
KAFKA_CONNECT_OPENSEARCH_VERSION=0.0.1
curl -o /tmp/kafka-connect-opensearch.jar https://github.com/dmathieu/kafka-connect-opensearch/releases/download/${KAFKA_CONNECT_OPENSEARCH_VERSION}/kafka-connect-opensearch.jar
```

Once you have the JAR file, move it to a path recognised by Java, such as `/usr/share/java/`.

## Configuration

Once the JAR is installed, you can configure a connector  using it. Create your connector using the `com.dmathieu.kafka.opensearch.OpenSearchSinkConnector` class.

```
{
  [...]
  "connector.class": "com.dmathieu.kafka.opensearch.OpenSearchSinkConnector",
  [...]
}
```

# About the license

Everything coming from the Confluent fork is licensed under the [Confluent Community License](LICENSE).  
Everything that has changed from the Elasticsearch version is licensed under the MIT license.
