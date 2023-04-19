/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.geospatial.ip2geo.common.DatasourceHelper;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataHelper;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action to create datasource
 */
@Log4j2
public class PutDatasourceTransportAction extends HandledTransportAction<PutDatasourceRequest, AcknowledgedResponse> {
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    private TimeValue timeout;
    private int indexingBulkSize;

    /**
     * Default constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param client the client
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param settings the settings
     * @param clusterSettings the cluster settings
     */
    @Inject
    public PutDatasourceTransportAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final Settings settings,
        final ClusterSettings clusterSettings
    ) {
        super(PutDatasourceAction.NAME, transportService, actionFilters, PutDatasourceRequest::new);
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        timeout = Ip2GeoSettings.TIMEOUT_IN_SECONDS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(Ip2GeoSettings.TIMEOUT_IN_SECONDS, it -> timeout = it);
        indexingBulkSize = Ip2GeoSettings.INDEXING_BULK_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(Ip2GeoSettings.INDEXING_BULK_SIZE, it -> indexingBulkSize = it);
    }

    @Override
    protected void doExecute(final Task task, final PutDatasourceRequest request, final ActionListener<AcknowledgedResponse> listener) {
        try {
            Datasource jobParameter = Datasource.Builder.build(request);
            IndexRequest indexRequest = new IndexRequest().index(DatasourceExtension.JOB_INDEX_NAME)
                .id(jobParameter.getName())
                .source(jobParameter.toXContent(JsonXContent.contentBuilder(), null))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .opType(DocWriteRequest.OpType.CREATE);
            client.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(final IndexResponse indexResponse) {
                    threadPool.generic().submit(() -> {
                        try {
                            createDatasource(jobParameter);
                        } catch (Exception e) {
                            log.error("Failed to create datasource for {}", jobParameter.getName(), e);
                            jobParameter.getUpdateStats().setLastFailedAt(Instant.now());
                            jobParameter.setState(DatasourceState.FAILED);
                            try {
                                DatasourceHelper.updateDatasource(client, jobParameter, timeout);
                            } catch (Exception ex) {
                                log.error("Failed to mark datasource state as FAILED for {}", jobParameter.getName(), ex);
                            }
                        }
                    });
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(final Exception e) {
                    if (e instanceof VersionConflictEngineException) {
                        log.info("Datasource already exists {}", request.getName(), e);
                        listener.onFailure(new ResourceAlreadyExistsException("Datasource already exists"));
                    } else {
                        log.error("Failed to create a datasource {}", request.getName(), e);
                        listener.onFailure(new OpenSearchException("Failed to create a datasource"));
                    }
                }
            });
        } catch (Exception e) {
            log.error("Error occurred while creating datasource {}", request.getName(), e);
            listener.onFailure(new OpenSearchException("Failed to create a datasource"));
        }
    }

    private void createDatasource(final Datasource jobParameter) throws Exception {
        if (!DatasourceState.PREPARING.equals(jobParameter.getState())) {
            log.error("Invalid datasource state. Expecting {} but received {}", DatasourceState.AVAILABLE, jobParameter.getState());
            jobParameter.setState(DatasourceState.FAILED);
            jobParameter.getUpdateStats().setLastFailedAt(Instant.now());
            DatasourceHelper.updateDatasource(client, jobParameter, timeout);
            return;
        }

        URL url = new URL(jobParameter.getEndpoint());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(url);

        Instant startTime = Instant.now();
        String indexName = jobParameter.indexNameFor(manifest);
        jobParameter.getIndices().add(indexName);
        DatasourceHelper.updateDatasource(client, jobParameter, timeout);
        GeoIpDataHelper.createIndex(clusterService, client, indexName, timeout);
        String[] fields;
        try (CSVParser reader = GeoIpDataHelper.getDatabaseReader(manifest)) {
            Iterator<CSVRecord> iter = reader.iterator();
            fields = iter.next().values();
            GeoIpDataHelper.putGeoData(client, indexName, fields, iter, indexingBulkSize, timeout);
        }

        Instant endTime = Instant.now();
        jobParameter.setDatabase(
            new Datasource.Database(
                manifest.getProvider(),
                manifest.getMd5Hash(),
                Instant.ofEpochMilli(manifest.getUpdatedAt()),
                manifest.getValidForInDays(),
                Arrays.asList(fields)
            )
        );
        jobParameter.getUpdateStats().setLastSucceededAt(endTime);
        jobParameter.getUpdateStats().setLastProcessingTimeInMillis(endTime.toEpochMilli() - startTime.toEpochMilli());
        jobParameter.enable();
        jobParameter.setState(DatasourceState.AVAILABLE);
        DatasourceHelper.updateDatasource(client, jobParameter, timeout);
        log.info(
            "GeoIP database creation succeeded for {} and took {} seconds",
            jobParameter.getName(),
            Duration.between(startTime, endTime)
        );
    }
}
