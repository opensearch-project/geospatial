/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.opensearch.OpenSearchException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.geospatial.shared.StashedThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;

/**
 * Facade class for datasource
 */
@Log4j2
public class DatasourceFacade {
    private static final Integer MAX_SIZE = 1000;
    private static final Tuple<String, Integer> INDEX_SETTING_NUM_OF_SHARDS = new Tuple<>("index.number_of_shards", 1);
    private static final Tuple<String, String> INDEX_SETTING_AUTO_EXPAND_REPLICAS = new Tuple<>("index.auto_expand_replicas", "0-all");
    private static final Tuple<String, Boolean> INDEX_SETTING_HIDDEN = new Tuple<>("index.hidden", true);
    private final Client client;
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;

    public DatasourceFacade(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        this.clusterSettings = clusterService.getClusterSettings();
    }

    /**
     * Create a datasource index of single shard with auto expand replicas to all nodes
     *
     * We want the index to expand to all replica so that datasource query request can be executed locally
     * for faster ingestion time.
     */
    public void createIndexIfNotExists(final StepListener<Void> stepListener) {
        if (clusterService.state().metadata().hasIndex(DatasourceExtension.JOB_INDEX_NAME) == true) {
            stepListener.onResponse(null);
            return;
        }
        final Map<String, Object> indexSettings = new HashMap<>();
        indexSettings.put(INDEX_SETTING_NUM_OF_SHARDS.v1(), INDEX_SETTING_NUM_OF_SHARDS.v2());
        indexSettings.put(INDEX_SETTING_AUTO_EXPAND_REPLICAS.v1(), INDEX_SETTING_AUTO_EXPAND_REPLICAS.v2());
        indexSettings.put(INDEX_SETTING_HIDDEN.v1(), INDEX_SETTING_HIDDEN.v2());
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(DatasourceExtension.JOB_INDEX_NAME).mapping(getIndexMapping())
            .settings(indexSettings);
        StashedThreadContext.run(client, () -> client.admin().indices().create(createIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(final CreateIndexResponse createIndexResponse) {
                stepListener.onResponse(null);
            }

            @Override
            public void onFailure(final Exception e) {
                if (e instanceof ResourceAlreadyExistsException) {
                    log.info("index[{}] already exist", DatasourceExtension.JOB_INDEX_NAME);
                    stepListener.onResponse(null);
                    return;
                }
                stepListener.onFailure(e);
            }
        }));
    }

    private String getIndexMapping() {
        try {
            try (InputStream is = DatasourceFacade.class.getResourceAsStream("/mappings/ip2geo_datasource.json")) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    return reader.lines().map(String::trim).collect(Collectors.joining());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update datasource in an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param datasource the datasource
     * @return index response
     */
    public IndexResponse updateDatasource(final Datasource datasource) {
        datasource.setLastUpdateTime(Instant.now());
        return StashedThreadContext.run(client, () -> {
            try {
                return client.prepareIndex(DatasourceExtension.JOB_INDEX_NAME)
                    .setId(datasource.getName())
                    .setOpType(DocWriteRequest.OpType.INDEX)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setSource(datasource.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .execute()
                    .actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Put datasource in an index {@code DatasourceExtension.JOB_INDEX_NAME}
     *
     * @param datasource the datasource
     * @param listener the listener
     */
    public void putDatasource(final Datasource datasource, final ActionListener listener) {
        datasource.setLastUpdateTime(Instant.now());
        StashedThreadContext.run(client, () -> {
            try {
                client.prepareIndex(DatasourceExtension.JOB_INDEX_NAME)
                    .setId(datasource.getName())
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setSource(datasource.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .execute(listener);
            } catch (IOException e) {
                new RuntimeException(e);
            }
        });
    }

    /**
     * Delete datasource in an index {@code DatasourceExtension.JOB_INDEX_NAME}
     *
     * @param datasource the datasource
     *
     */
    public void deleteDatasource(final Datasource datasource) {
        if (client.admin()
            .indices()
            .prepareDelete(datasource.getIndices().toArray(new String[0]))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .execute()
            .actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT))
            .isAcknowledged() == false) {
            throw new OpenSearchException("failed to delete data[{}] in datasource", String.join(",", datasource.getIndices()));
        }
        DeleteResponse response = client.prepareDelete()
            .setIndex(DatasourceExtension.JOB_INDEX_NAME)
            .setId(datasource.getName())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));

        if (response.status().equals(RestStatus.OK)) {
            log.info("deleted datasource[{}] successfully", datasource.getName());
        } else if (response.status().equals(RestStatus.NOT_FOUND)) {
            throw new ResourceNotFoundException("datasource[{}] does not exist", datasource.getName());
        } else {
            throw new OpenSearchException("failed to delete datasource[{}] with status[{}]", datasource.getName(), response.status());
        }
    }

    /**
     * Get datasource from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param name the name of a datasource
     * @return datasource
     * @throws IOException exception
     */
    public Datasource getDatasource(final String name) throws IOException {
        GetRequest request = new GetRequest(DatasourceExtension.JOB_INDEX_NAME, name);
        GetResponse response;
        try {
            response = StashedThreadContext.run(client, () -> client.get(request).actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT)));
            if (response.isExists() == false) {
                log.error("Datasource[{}] does not exist in an index[{}]", name, DatasourceExtension.JOB_INDEX_NAME);
                return null;
            }
        } catch (IndexNotFoundException e) {
            log.error("Index[{}] is not found", DatasourceExtension.JOB_INDEX_NAME);
            return null;
        }

        XContentParser parser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getSourceAsBytesRef()
        );
        return Datasource.PARSER.parse(parser, null);
    }

    /**
     * Get datasource from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param name the name of a datasource
     * @param actionListener the action listener
     */
    public void getDatasource(final String name, final ActionListener<Datasource> actionListener) {
        GetRequest request = new GetRequest(DatasourceExtension.JOB_INDEX_NAME, name);
        StashedThreadContext.run(client, () -> client.get(request, new ActionListener<>() {
            @Override
            public void onResponse(final GetResponse response) {
                if (response.isExists() == false) {
                    actionListener.onResponse(null);
                    return;
                }

                try {
                    XContentParser parser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        response.getSourceAsBytesRef()
                    );
                    actionListener.onResponse(Datasource.PARSER.parse(parser, null));
                } catch (IOException e) {
                    actionListener.onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        }));
    }

    /**
     * Get datasources from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param names the array of datasource names
     * @param actionListener the action listener
     */
    public void getDatasources(final String[] names, final ActionListener<List<Datasource>> actionListener) {
        StashedThreadContext.run(
            client,
            () -> client.prepareMultiGet()
                .add(DatasourceExtension.JOB_INDEX_NAME, names)
                .execute(createGetDataSourceQueryActionLister(MultiGetResponse.class, actionListener))
        );
    }

    /**
     * Get all datasources up to {@code MAX_SIZE} from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param actionListener the action listener
     */
    public void getAllDatasources(final ActionListener<List<Datasource>> actionListener) {
        StashedThreadContext.run(
            client,
            () -> client.prepareSearch(DatasourceExtension.JOB_INDEX_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(MAX_SIZE)
                .execute(createGetDataSourceQueryActionLister(SearchResponse.class, actionListener))
        );
    }

    private <T> ActionListener<T> createGetDataSourceQueryActionLister(
        final Class<T> response,
        final ActionListener<List<Datasource>> actionListener
    ) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(final T response) {
                try {
                    List<BytesReference> bytesReferences = toBytesReferences(response);
                    List<Datasource> datasources = bytesReferences.stream()
                        .map(bytesRef -> toDatasource(bytesRef))
                        .collect(Collectors.toList());
                    actionListener.onResponse(datasources);
                } catch (Exception e) {
                    actionListener.onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        };
    }

    private List<BytesReference> toBytesReferences(final Object response) {
        if (response instanceof SearchResponse) {
            SearchResponse searchResponse = (SearchResponse) response;
            return Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getSourceRef).collect(Collectors.toList());
        } else if (response instanceof MultiGetResponse) {
            MultiGetResponse multiGetResponse = (MultiGetResponse) response;
            return Arrays.stream(multiGetResponse.getResponses())
                .map(MultiGetItemResponse::getResponse)
                .filter(Objects::nonNull)
                .filter(GetResponse::isExists)
                .map(GetResponse::getSourceAsBytesRef)
                .collect(Collectors.toList());
        } else {
            throw new OpenSearchException("No supported instance type[{}] is provided", response.getClass());
        }
    }

    private Datasource toDatasource(final BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                bytesReference
            );
            return Datasource.PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
