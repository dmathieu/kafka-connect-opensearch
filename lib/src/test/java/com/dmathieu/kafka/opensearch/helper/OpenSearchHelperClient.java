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

package com.dmathieu.kafka.opensearch.helper;

import org.apache.http.HttpHost;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.DataStream;
import org.opensearch.client.indices.DeleteDataStreamRequest;
import org.opensearch.client.indices.GetDataStreamRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.security.PutRoleRequest;
import org.opensearch.client.security.PutRoleResponse;
import org.opensearch.client.security.PutUserRequest;
import org.opensearch.client.security.PutUserResponse;
import org.opensearch.client.security.RefreshPolicy;
import org.opensearch.client.security.user.User;
import org.opensearch.client.security.user.privileges.Role;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import com.dmathieu.kafka.opensearch.ConfigCallbackHandler;
import com.dmathieu.kafka.opensearch.OpenSearchSinkConnectorConfig;

public class OpenSearchHelperClient {

  private static final Logger log = LoggerFactory.getLogger(OpenSearchHelperClient.class);

  private RestHighLevelClient client;

  public OpenSearchHelperClient(String url, OpenSearchSinkConnectorConfig config) {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    this.client = new RestHighLevelClient(
        RestClient
            .builder(HttpHost.create(url))
            .setHttpClientConfigCallback(configCallbackHandler)
    );
  }

  public void deleteIndex(String index, boolean isDataStream) throws IOException {
    if (isDataStream) {
      DeleteDataStreamRequest request = new DeleteDataStreamRequest(index);
      client.indices().deleteDataStream(request, RequestOptions.DEFAULT);
      return;
    }
    DeleteIndexRequest request = new DeleteIndexRequest(index);
    client.indices().delete(request, RequestOptions.DEFAULT);
  }

  public DataStream getDataStream(String dataStream) throws IOException {
    GetDataStreamRequest request = new GetDataStreamRequest(dataStream);
    List<DataStream> datastreams = client.indices()
        .getDataStream(request, RequestOptions.DEFAULT)
        .getDataStreams();
    return datastreams.size() == 0 ? null : datastreams.get(0);
  }

  public long getDocCount(String index) throws IOException {
    CountRequest request = new CountRequest(index);
    return client.count(request, RequestOptions.DEFAULT).getCount();
  }

  public MappingMetadata getMapping(String index) throws IOException {
    GetMappingsRequest request = new GetMappingsRequest().indices(index);
    GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
    return response.mappings().get(index);
  }

  public boolean indexExists(String index) throws IOException {
    GetIndexRequest request = new GetIndexRequest(index);
    return client.indices().exists(request, RequestOptions.DEFAULT);
  }

  public void createIndex(String index, String jsonMappings) throws IOException {
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(index).mapping(jsonMappings, XContentType.JSON);
    client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
  }

  public SearchHits search(String index) throws IOException {
    SearchRequest request = new SearchRequest(index);
    return client.search(request, RequestOptions.DEFAULT).getHits();
  }

  public void createRole(Role role) throws IOException {
    PutRoleRequest putRoleRequest = new PutRoleRequest(role, RefreshPolicy.IMMEDIATE);
    PutRoleResponse putRoleResponse = client.security().putRole(putRoleRequest, RequestOptions.DEFAULT);
    if (!putRoleResponse.isCreated()) {
      throw new RuntimeException(String.format("Failed to create a role %s", role.getName()));
    }
  }

  public void createUser(Entry<User, String> userToPassword) throws IOException {
    PutUserRequest putUserRequest = PutUserRequest.withPassword(
        userToPassword.getKey(),
        userToPassword.getValue().toCharArray(),
        true,
        RefreshPolicy.IMMEDIATE
    );
    PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
    if (!putUserResponse.isCreated()) {
      throw new RuntimeException(String.format("Failed to create a user %s", userToPassword.getKey().getUsername()));
    }
  }

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      log.error("Error closing client.", e);
    }
  }
}
