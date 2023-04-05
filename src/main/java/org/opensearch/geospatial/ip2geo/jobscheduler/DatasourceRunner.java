/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.ip2geo.common.DatasourceHelper;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataHelper;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.ThreadPool;

/**
 * Datasource update task
 *
 * This is a background task which is responsible for updating Ip2Geo datasource
 */
@Log4j2
public class DatasourceRunner implements ScheduledJobRunner {

    private static DatasourceRunner INSTANCE;

    /**
     * Return a singleton job runner instance
     * @return job runner
     */
    public static DatasourceRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (DatasourceRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new DatasourceRunner();
            return INSTANCE;
        }
    }

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private TimeValue timeout;
    private int indexingBulkSize;

    private DatasourceRunner() {
        // Singleton class, use getJobRunner method instead of constructor
    }

    /**
     * Set cluster service
     * @param clusterService the cluster service
     */
    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;

        timeout = Ip2GeoSettings.TIMEOUT_IN_SECONDS.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(Ip2GeoSettings.TIMEOUT_IN_SECONDS, it -> timeout = it);
        indexingBulkSize = Ip2GeoSettings.INDEXING_BULK_SIZE.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(Ip2GeoSettings.INDEXING_BULK_SIZE, it -> indexingBulkSize = it);
    }

    /**
     * Set thread pool
     * @param threadPool the thread pool
     */
    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Set client
     * @param client the client
     */
    public void setClient(Client client) {
        this.client = client;
    }

    /**
     * Update GeoIP data
     *
     * Lock is used so that only one of nodes run this task.
     * Lock duration is 1 hour to avoid refreshing. This is okay because update interval is 1 day minimum.
     *
     * @param jobParameter job parameter
     * @param context context
     */
    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        log.info("Update job started for a datasource[{}]", jobParameter.getName());
        if (!(jobParameter instanceof Datasource)) {
            throw new IllegalStateException(
                "Job parameter is not instance of DatasourceUpdateJobParameter, type: " + jobParameter.getClass().getCanonicalName()
            );
        }

        if (clusterService == null) {
            throw new IllegalStateException("ClusterService is not initialized.");
        }

        if (threadPool == null) {
            throw new IllegalStateException("ThreadPool is not initialized.");
        }

        if (client == null) {
            throw new IllegalStateException("Client is not initialized.");
        }

        final LockService lockService = context.getLockService();
        Runnable runnable = () -> {
            if (jobParameter.getLockDurationSeconds() != null) {
                lockService.acquireLock(jobParameter, context, ActionListener.wrap(lock -> {
                    if (lock == null) {
                        return;
                    }
                    Datasource parameter = (Datasource) jobParameter;
                    try {
                        deleteUnusedIndices(parameter);
                        updateDatasource(parameter);
                        deleteUnusedIndices(parameter);
                    } catch (Exception e) {
                        log.error("Failed to update datasource for {}", parameter.getId(), e);
                        parameter.getUpdateStats().setLastFailedAt(Instant.now());
                        DatasourceHelper.updateDatasource(client, parameter, timeout);
                    } finally {
                        lockService.release(
                            lock,
                            ActionListener.wrap(
                                released -> { log.info("Released lock for job {}", jobParameter.getName()); },
                                exception -> { throw new IllegalStateException("Failed to release lock."); }
                            )
                        );
                    }
                }, exception -> { throw new IllegalStateException("Failed to acquire lock."); }));
            }
        };

        threadPool.generic().submit(runnable);
    }

    /**
     * Delete all indices except the one which are being used
     *
     * @param parameter
     */
    private void deleteUnusedIndices(final Datasource parameter) {
        try {
            List<String> deletedIndices = new ArrayList<>();
            for (String index : parameter.getIndices()) {
                if (index.equals(parameter.currentIndexName())) {
                    continue;
                }

                if (!clusterService.state().metadata().hasIndex(index)) {
                    deletedIndices.add(index);
                    continue;
                }

                try {
                    if (client.admin()
                        .indices()
                        .prepareDelete(index)
                        .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                        .execute()
                        .actionGet(timeout)
                        .isAcknowledged()) {
                        deletedIndices.add(index);
                    } else {
                        log.error("Failed to delete an index {}", index);
                    }
                } catch (Exception e) {
                    log.error("Failed to delete an index {}", index, e);
                }
            }
            if (!deletedIndices.isEmpty()) {
                parameter.getIndices().removeAll(deletedIndices);
                DatasourceHelper.updateDatasource(client, parameter, timeout);
            }
        } catch (Exception e) {
            log.error("Failed to delete old indices for {}", parameter.getId(), e);
        }
    }

    /**
     * Update GeoIP data internal
     *
     * @param jobParameter
     * @throws Exception
     */
    private void updateDatasource(final Datasource jobParameter) throws Exception {
        if (!DatasourceState.AVAILABLE.equals(jobParameter.getState())) {
            log.error("Invalid datasource state. Expecting {} but received {}", DatasourceState.AVAILABLE, jobParameter.getState());
            jobParameter.disable();
            jobParameter.getUpdateStats().setLastFailedAt(Instant.now());
            DatasourceHelper.updateDatasource(client, jobParameter, timeout);
            return;
        }

        URL url = new URL(jobParameter.getEndpoint());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(url);

        if (skipUpdate(jobParameter, manifest)) {
            log.info("Skipping GeoIP database update. Update is not required for {}", jobParameter.getId());
            jobParameter.getUpdateStats().setLastSkippedAt(Instant.now());
            DatasourceHelper.updateDatasource(client, jobParameter, timeout);
            return;
        }

        Instant startTime = Instant.now();
        String indexName = jobParameter.indexNameFor(manifest);
        jobParameter.getIndices().add(indexName);
        DatasourceHelper.updateDatasource(client, jobParameter, timeout);
        GeoIpDataHelper.createIndex(clusterService, client, indexName, timeout);
        String[] fields;
        try (CSVParser reader = GeoIpDataHelper.getDatabaseReader(manifest)) {
            Iterator<CSVRecord> iter = reader.iterator();
            fields = iter.next().values();
            if (!jobParameter.getDatabase().getFields().equals(Arrays.asList(fields))) {
                log.error("The previous fields and new fields does not match.");
                log.error("Previous: {}, New: {}", jobParameter.getDatabase().getFields().toString(), Arrays.asList(fields).toString());
                throw new IllegalStateException("Fields does not match between old and new");
            }
            GeoIpDataHelper.putGeoData(client, indexName, fields, iter, indexingBulkSize, timeout);
        }

        Instant endTime = Instant.now();
        jobParameter.getDatabase().setProvider(manifest.getProvider());
        jobParameter.getDatabase().setMd5Hash(manifest.getMd5Hash());
        jobParameter.getDatabase().setUpdatedAt(Instant.ofEpochMilli(manifest.getUpdatedAt()));
        jobParameter.getDatabase().setValidForInDays(manifest.getValidForInDays());
        jobParameter.getDatabase().setFields(Arrays.asList(fields));
        jobParameter.getUpdateStats().setLastSucceededAt(endTime);
        jobParameter.getUpdateStats().setLastProcessingTimeInMillis(endTime.toEpochMilli() - startTime.toEpochMilli());
        DatasourceHelper.updateDatasource(client, jobParameter, timeout);
        log.info(
            "GeoIP database creation succeeded for {} and took {} seconds",
            jobParameter.getId(),
            Duration.between(startTime, endTime)
        );
    }

    /**
     * Determine if update is needed or not
     *
     * Update is needed when all following conditions are met
     * 1. MD5 hash value in datasource is different with MD5 hash value in manifest
     * 2. updatedAt value in datasource is before updateAt value in manifest
     *
     * @param parameter
     * @param manifest
     * @return
     */
    private boolean skipUpdate(final Datasource parameter, final DatasourceManifest manifest) {
        if (manifest.getMd5Hash().equals(parameter.getDatabase().getMd5Hash())) {
            return true;
        }

        return parameter.getDatabase().getUpdatedAt().toEpochMilli() >= manifest.getUpdatedAt();
    }
}
