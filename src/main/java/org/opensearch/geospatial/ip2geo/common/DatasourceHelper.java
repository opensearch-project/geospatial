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
import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j2;

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
import org.opensearch.common.unit.TimeValue;
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
 * Helper class for datasource
 */
@Log4j2
public class DatasourceHelper {

    /**
     * Update datasource in an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param client the client
     * @param datasource the datasource
     * @param timeout the timeout
     * @return index response
     * @throws IOException exception
     */
    public static IndexResponse updateDatasource(final Client client, final Datasource datasource, final TimeValue timeout)
        throws IOException {
        datasource.setLastUpdateTime(Instant.now());
        IndexRequestBuilder requestBuilder = client.prepareIndex(DatasourceExtension.JOB_INDEX_NAME);
        requestBuilder.setId(datasource.getName());
        requestBuilder.setOpType(DocWriteRequest.OpType.INDEX);
        requestBuilder.setSource(datasource.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        return client.index(requestBuilder.request()).actionGet(timeout);
    }

    /**
     * Get datasource from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param client the client
     * @param name the name of a datasource
     * @param timeout the timeout
     * @return datasource
     * @throws IOException exception
     */
    public static Datasource getDatasource(final Client client, final String name, final TimeValue timeout) throws IOException {
        GetRequest request = new GetRequest(DatasourceExtension.JOB_INDEX_NAME, name);
        GetResponse response;
        try {
            response = client.get(request).actionGet(timeout);
            if (!response.isExists()) {
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
     * @param client the client
     * @param name the name of a datasource
     * @param actionListener the action listener
     */
    public static void getDatasource(final Client client, final String name, final ActionListener<Datasource> actionListener) {
        GetRequest request = new GetRequest(DatasourceExtension.JOB_INDEX_NAME, name);
        client.get(request, new ActionListener<>() {
            @Override
            public void onResponse(final GetResponse response) {
                if (!response.isExists()) {
                    actionListener.onResponse(null);
                } else {
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
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    /**
     * Get datasources from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param client the client
     * @param names the array of datasource names
     * @param actionListener the action listener
     */
    public static void getDatasources(final Client client, final String[] names, final ActionListener<List<Datasource>> actionListener) {
        client.prepareMultiGet().add(DatasourceExtension.JOB_INDEX_NAME, names).execute(new ActionListener<>() {
            @Override
            public void onResponse(final MultiGetResponse response) {
                List<Datasource> datasources = new ArrayList<>(response.getResponses().length);
                for (MultiGetItemResponse item : response.getResponses()) {
                    if (item.getResponse() != null && item.getResponse().isExists()) {
                        try {
                            XContentParser parser = XContentHelper.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                item.getResponse().getSourceAsBytesRef()
                            );
                            datasources.add(Datasource.PARSER.parse(parser, null));
                        } catch (IOException e) {
                            actionListener.onFailure(e);
                            return;
                        }
                    }
                }
                actionListener.onResponse(datasources);
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    /**
     * Get all datasources from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param client the client
     * @param actionListener the action listener
     */
    public static void getAllDatasources(final Client client, final ActionListener<List<Datasource>> actionListener) {
        client.prepareSearch(DatasourceExtension.JOB_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).execute(new ActionListener<>() {
            @Override
            public void onResponse(final SearchResponse searchResponse) {
                List<Datasource> datasources = new ArrayList<>(searchResponse.getHits().getHits().length);
                for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                    try {
                        XContentParser parser = XContentHelper.createParser(
                            NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE,
                            searchHit.getSourceRef()
                        );
                        datasources.add(Datasource.PARSER.parse(parser, null));
                    } catch (IOException e) {
                        actionListener.onFailure(e);
                        return;
                    }
                }
                actionListener.onResponse(datasources);
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        });
    }
}
