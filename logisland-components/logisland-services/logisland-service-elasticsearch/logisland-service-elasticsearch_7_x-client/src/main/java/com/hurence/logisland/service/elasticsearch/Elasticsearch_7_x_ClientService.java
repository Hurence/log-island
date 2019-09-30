/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.elasticsearch;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

@Tags({ "elasticsearch", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for ElasticSearch 7.x. Note that although " +
        "Elasticsearch 7.x still accepts type information, this implementation will ignore any type usage and " +
        "will only work at the index level to be already compliant with the ElasticSearch 8.x version that will " +
        "completely remove type usage.")
public class Elasticsearch_7_x_ClientService extends AbstractControllerService implements ElasticsearchClientService {


    protected volatile RestHighLevelClient esClient;
    private volatile HttpHost[] esHosts;
    private volatile String authToken;
    protected volatile BulkProcessor bulkProcessor;
    protected volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(BULK_BACK_OFF_POLICY);
        props.add(BULK_THROTTLING_DELAY);
        props.add(BULK_RETRY_NUMBER);
        props.add(BATCH_SIZE);
        props.add(BULK_SIZE);
        props.add(FLUSH_INTERVAL);
        props.add(CONCURRENT_REQUESTS);
        props.add(PING_TIMEOUT);
        props.add(SAMPLER_INTERVAL);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(PROP_SHIELD_LOCATION);
        props.add(HOSTS);
        props.add(PROP_SSL_CONTEXT_SERVICE);
        props.add(CHARSET);

        return Collections.unmodifiableList(props);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        super.init(context);
        synchronized(this) {
            try {
                createElasticsearchClient(context);
                createBulkProcessor(context);
            } catch (Exception e){
                throw new InitializationException(e);
            }
        }
    }

    /**
     * Instantiate ElasticSearch Client. This should be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    protected void createElasticsearchClient(ControllerServiceInitializationContext context) throws ProcessException {
        if (esClient != null) {
            return;
        }

        try {
            final String username = context.getPropertyValue(USERNAME).asString();
            final String password = context.getPropertyValue(PASSWORD).asString();
            final String hosts = context.getPropertyValue(HOSTS).asString();

            esHosts = getEsHosts(hosts);

            if (esHosts != null) {

                RestClientBuilder builder = RestClient.builder(esHosts);

                if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

                    builder.setHttpClientConfigCallback(httpClientBuilder -> {
                                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                });
                }

                esClient = new RestHighLevelClient(builder);
            }

        } catch (Exception e) {
            getLogger().error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the ElasticSearch hosts.
     *
     * @param hosts A comma-separated list of ElasticSearch hosts (host:port,host2:port2, etc.)
     * @return List of HttpHost for the ES hosts
     */
    private HttpHost[]  getEsHosts(String hosts) {

        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        HttpHost[] esHosts = new HttpHost[esList.size()];
        int indHost = 0;

        for (String item : esList) {
            String[] addresses = item.split(":");
            final String hostName = addresses[0].trim();
            final int port = Integer.parseInt(addresses[1].trim());

            esHosts[indHost] = new HttpHost(hostName, port);
            indHost++;
        }
        return esHosts;
    }


    protected void createBulkProcessor(ControllerServiceInitializationContext context)
    {
        if (bulkProcessor != null) {
            return;
        }

        // create the bulk processor

       BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                getLogger().debug("Going to execute bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                getLogger().debug("Executed bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
                if (bulkResponse.hasFailures()) {
                    getLogger().warn("There was failures while executing bulk [id:{}]," +
                                    " done bulk request in {} ms with failure = {}",
                            new Object[]{l, bulkResponse.getTook().getMillis(), bulkResponse.buildFailureMessage()});
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            errors.put(item.getId(), item.getFailureMessage());
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                getLogger().error("something went wrong while bulk loading events to es : {}", new Object[]{throwable.getMessage()});
            }

        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) -> esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        bulkProcessor = BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(context.getPropertyValue(BATCH_SIZE).asInteger())
                .setBulkSize(new ByteSizeValue(context.getPropertyValue(BULK_SIZE).asInteger(), ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(context.getPropertyValue(FLUSH_INTERVAL).asInteger()))
                .setConcurrentRequests(context.getPropertyValue(CONCURRENT_REQUESTS).asInteger())
                .setBackoffPolicy(getBackOffPolicy(context))
                .build();
    }

    /**
     * set up BackoffPolicy
     */
    private BackoffPolicy getBackOffPolicy(ControllerServiceInitializationContext context)
    {
        BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();
        if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(DEFAULT_EXPONENTIAL_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.exponentialBackoff();
        } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(EXPONENTIAL_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.exponentialBackoff(
                    TimeValue.timeValueMillis(context.getPropertyValue(BULK_THROTTLING_DELAY).asLong()),
                    context.getPropertyValue(BULK_RETRY_NUMBER).asInteger()
            );
        } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(CONSTANT_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.constantBackoff(
                    TimeValue.timeValueMillis(context.getPropertyValue(BULK_THROTTLING_DELAY).asLong()),
                    context.getPropertyValue(BULK_RETRY_NUMBER).asInteger()
            );
        } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(NO_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.noBackoff();
        }
        return backoffPolicy;
    }


    @Override
    public void bulkFlush() throws DatastoreClientServiceException {
        bulkProcessor.flush();
    }

    @Override
    public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {

        // Note: we do not support type anymore but keep it in API (method signature) for backward compatibility
        // purpose. So the type is ignored, even if filled.

        // add it to the bulk,
        IndexRequest request = new IndexRequest(docIndex)
                .source(document, XContentType.JSON)
                .opType(IndexRequest.OpType.INDEX);

        if(OptionalId.isPresent()){
            request.id(OptionalId.get());
        }

        bulkProcessor.add(request);
    }

    @Override
    public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {

        // Note: we do not support type anymore but keep it in API (method signature) for backward compatibility
        // purpose. So the type is ignored, even if filled.


        // add it to the bulk
        IndexRequest request = new IndexRequest(docIndex)
                .source(document)
                .opType(IndexRequest.OpType.INDEX);

        if(OptionalId.isPresent()){
            request.id(OptionalId.get());
        }

        bulkProcessor.add(request);
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {

        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();

        MultiGetRequest multiGetRequest = new MultiGetRequest();

        for (MultiGetQueryRecord multiGetQueryRecord : multiGetQueryRecords)
        {
            String index = multiGetQueryRecord.getIndexName();
            List<String> documentIds = multiGetQueryRecord.getDocumentIds();
            String[] fieldsToInclude = multiGetQueryRecord.getFieldsToInclude();
            String[] fieldsToExclude = multiGetQueryRecord.getFieldsToExclude();
            if ((fieldsToInclude != null && fieldsToInclude.length > 0) || (fieldsToExclude != null && fieldsToExclude.length > 0)) {
                for (String documentId : documentIds) {
                    MultiGetRequest.Item item = new MultiGetRequest.Item(index, documentId);
                    item.fetchSourceContext(new FetchSourceContext(true, fieldsToInclude, fieldsToExclude));
                    multiGetRequest.add(item);
                }
            } else {
                for (String documentId : documentIds) {
                    multiGetRequest.add(index, documentId);
                }
            }
        }

        MultiGetResponse multiGetItemResponses = null;
        try {
            multiGetItemResponses = esClient.mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            getLogger().error("MultiGet query failed : {}", new Object[]{e.getMessage()});
        }

        if (multiGetItemResponses != null) {
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                if (response != null && response.isExists()) {
                    Map<String,Object> responseMap = response.getSourceAsMap();
                    Map<String,String> retrievedFields = new HashMap<>();
                    responseMap.forEach((k,v) -> {if (v!=null) retrievedFields.put(k, v.toString());});
                    multiGetResponseRecords.add(new MultiGetResponseRecord(response.getIndex(), response.getType(), response.getId(), retrievedFields));
                }
            }
        }

        return multiGetResponseRecords;
    }

    @Override
    public boolean existsCollection(String indexName) throws DatastoreClientServiceException {
        boolean exists;
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            exists = esClient.indices().exists(request, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
        return exists;
    }

    @Override
    public void refreshCollection(String indexName) throws DatastoreClientServiceException {
        try {
            RefreshRequest request = new RefreshRequest(indexName);
            esClient.indices().refresh(request, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {
        IndexRequest indexRequest = new IndexRequest(indexName).source(doc);
        esClient.index(indexRequest, RequestOptions.DEFAULT);
        refreshCollection(indexName);
    }


    @Override
    public long countCollection(String indexName) throws DatastoreClientServiceException {
        CountResponse countResponse;
        try {
            CountRequest countRequest = new CountRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            countRequest.source(searchSourceBuilder);
            countResponse = esClient.count(countRequest, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
        return countResponse.getCount();
    }

    @Override
    public void createCollection(String indexName, int numShards, int numReplicas) throws DatastoreClientServiceException {
        // Define the index itself
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        request.settings(Settings.builder()
                .put("index.number_of_shards", numShards)
                .put("index.number_of_replicas", numReplicas)
        );

        try {
            CreateIndexResponse rsp = esClient.indices().create(request, RequestOptions.DEFAULT);
            if (!rsp.isAcknowledged()) {
                throw new IOException("Elasticsearch index definition not acknowledged");
            }
            getLogger().info("Created index {}", new Object[]{indexName});
        } catch (Exception e) {
            getLogger().error("Failed to create ES index", e);
            throw new DatastoreClientServiceException("Failed to create ES index", e);
        }
    }

    @Override
    public void dropCollection(String indexName) throws DatastoreClientServiceException {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            getLogger().info("Delete index {}", new Object[]{indexName});
            esClient.indices().delete(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new DatastoreClientServiceException(String.format("Unable to delete index %s", indexName), e);
        }
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String srcIndex, String dstIndex) throws DatastoreClientServiceException {
        try {
            ReindexRequest request = new ReindexRequest().setSourceIndices(srcIndex).setDestIndex(dstIndex);
            esClient.reindex(request, RequestOptions.DEFAULT);
            getLogger().info("Reindex completed");
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void createAlias(String indexName, String aliasName) throws DatastoreClientServiceException {
        try {

            IndicesAliasesRequest request = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions aliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(indexName)
                            .alias(aliasName);
            request.addAliasAction(aliasAction);
            AcknowledgedResponse rsp = esClient.indices().updateAliases(request, RequestOptions.DEFAULT);

            if (!rsp.isAcknowledged()) {
                throw new DatastoreClientServiceException(String.format(
                        "Creation of elasticsearch alias '%s' for index '%s' not acknowledged.", aliasName, indexName));
            }
        } catch (DatastoreClientServiceException e) {
            getLogger().error("Failed to create elasticsearch alias {} for index {}", new Object[]{aliasName, indexName, e});
            throw e;
        }
        catch (Exception e){
            String msg = String.format("Failed to create elasticsearch alias '%s' for index '%s'", aliasName, indexName);
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        PutMappingRequest request = new PutMappingRequest(indexName);
        request.source(mappingAsJsonString, XContentType.JSON);

        try {
            AcknowledgedResponse rsp = esClient.indices().putMapping(request, RequestOptions.DEFAULT);
            if (!rsp.isAcknowledged()) {
                throw new DatastoreClientServiceException("Elasticsearch mapping definition not acknowledged");
            }
            return true;
        } catch (Exception e) {
            getLogger().error("Failed to put ES mapping for index {} : {}", new Object[]{indexName, e});
            // This is an error that can be fixed by providing alternative inputs so return boolean rather
            // than throwing an exception.
            return false;
        }
    }

    @Override
    public void bulkPut(String indexName, Record record) throws DatastoreClientServiceException {

        // Note: we do not support type anymore but keep it in API (method signature) for backward compatibility
        // purpose. So the type is ignored, even if filled. So we check for presence of the ',' separator.
        // If present, a type was provided and we ignore it, if not present, the whole string is used as the
        // index name.

        if (indexName.contains(","))
        {
            final List<String> indexType = Arrays.asList(indexName.split(","));
            if (indexType.size() == 2) {
                indexName = indexType.get(0);
                // Ignore type which is at index 1
            }
            else {
                throw new DatastoreClientServiceException("Could not parse index/type name although the provided " +
                        "string contains a ',' separator: " + indexName);
            }
        } else {

        }

        bulkPut(indexName, null, ElasticsearchRecordConverter.convertToString(record), Optional.of(record.getId()));
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public Collection<Record> query(String query) {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public long queryCount(String query) {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public String convertRecordToString(Record record) {
        return ElasticsearchRecordConverter.convertToString(record);
    }

    @Override
    public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) {

        long numberOfHits;

        try {
            SearchRequest searchRequest = new SearchRequest(docIndex);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            sourceBuilder.query(QueryBuilders.termQuery(docName, docValue));
            sourceBuilder.from(0);
            sourceBuilder.size(60);
            sourceBuilder.explain(false);

            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            numberOfHits = searchResponse.getHits().getTotalHits().value;
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }

        return numberOfHits;
    }

    @OnDisabled
    public void shutdown() {
        if (bulkProcessor != null) {
            bulkProcessor.flush();
            try {
                if (!bulkProcessor.awaitClose(10, TimeUnit.SECONDS)) {
                    getLogger().error("some request could not be send to es because of time out");
                } else {
                    getLogger().info("all requests have been submitted to es");
                }
            } catch (InterruptedException e) {
                getLogger().error(e.getMessage());
            }
        }

        if (esClient != null) {
            getLogger().info("Closing ElasticSearch Client");
            try {
                esClient.close();
            }
            catch (Exception e){
                throw new DatastoreClientServiceException(e);
            }
            esClient = null;
        }
    }
}