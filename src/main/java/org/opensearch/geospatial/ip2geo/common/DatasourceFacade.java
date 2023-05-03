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

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
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

/**
 * Facade class for datasource
 */
@Log4j2
public class DatasourceFacade {
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
        requestBuilder.setId(datasource.getId());
        requestBuilder.setOpType(DocWriteRequest.OpType.INDEX);
        requestBuilder.setSource(datasource.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        return client.index(requestBuilder.request()).actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));
    }

    /**
     * Get datasource from an index {@code DatasourceExtension.JOB_INDEX_NAME}
     * @param id the name of a datasource
     * @return datasource
     * @throws IOException exception
     */
    public Datasource getDatasource(final String id) throws IOException {
        GetRequest request = new GetRequest(DatasourceExtension.JOB_INDEX_NAME, id);
        GetResponse response;
        try {
            response = client.get(request).actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));
            if (response.isExists() == false) {
                log.error("Datasource[{}] does not exist in an index[{}]", id, DatasourceExtension.JOB_INDEX_NAME);
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
     * @param id the name of a datasource
     * @param actionListener the action listener
     */
    public void getDatasource(final String id, final ActionListener<Datasource> actionListener) {
        GetRequest request = new GetRequest(DatasourceExtension.JOB_INDEX_NAME, id);
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
}
