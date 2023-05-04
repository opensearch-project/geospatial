/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;

/**
 * Facade class for datasource
 */
@Log4j2
public class DatasourceFacade {
    private static final Integer MAX_SIZE = 1000;
    private final Client client;
    private final ClusterSettings clusterSettings;

    public DatasourceFacade(final Client client, final ClusterSettings clusterSettings) {
        this.client = client;
        this.clusterSettings = clusterSettings;
    }

    /**
     * Update datasource in an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param datasource the datasource
     * @return index response
     * @throws IOException exception
     */
    public IndexResponse updateDatasource(final Datasource datasource) throws IOException {
        datasource.setLastUpdateTime(Instant.now());
        IndexRequestBuilder requestBuilder = client.prepareIndex(DatasourceExtension.JOB_INDEX_NAME);
        requestBuilder.setId(datasource.getName());
        requestBuilder.setOpType(DocWriteRequest.OpType.INDEX);
        requestBuilder.setSource(datasource.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        return client.index(requestBuilder.request()).actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));
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
            response = client.get(request).actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));
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
        client.get(request, new ActionListener<GetResponse>() {
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
        });
    }

    /**
     * Get datasources from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param names the array of datasource names
     * @param actionListener the action listener
     */
    public void getDatasources(final String[] names, final ActionListener<List<Datasource>> actionListener) {
        client.prepareMultiGet()
            .add(DatasourceExtension.JOB_INDEX_NAME, names)
            .execute(createGetDataSourceQueryActionLister(MultiGetResponse.class, actionListener));
    }

    /**
     * Get all datasources up to {@code MAX_SIZE} from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param actionListener the action listener
     */
    public void getAllDatasources(final ActionListener<List<Datasource>> actionListener) {
        client.prepareSearch(DatasourceExtension.JOB_INDEX_NAME)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(MAX_SIZE)
            .execute(createGetDataSourceQueryActionLister(SearchResponse.class, actionListener));
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
