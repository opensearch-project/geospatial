/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.dao.DatasourceDao;
import org.opensearch.geospatial.ip2geo.dao.GeoIpDataDao;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

@Log4j2
public class DatasourceUpdateService {
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;
    private final DatasourceDao datasourceDao;
    private final GeoIpDataDao geoIpDataDao;

    public DatasourceUpdateService(
        final ClusterService clusterService,
        final DatasourceDao datasourceDao,
        final GeoIpDataDao geoIpDataDao
    ) {
        this.clusterService = clusterService;
        this.clusterSettings = clusterService.getClusterSettings();
        this.datasourceDao = datasourceDao;
        this.geoIpDataDao = geoIpDataDao;
    }

    /**
     * Update GeoIp data
     *
     * The first column is ip range field regardless its header name.
     * Therefore, we don't store the first column's header name.
     *
     * @param datasource the datasource
     * @param renewLock runnable to renew lock
     *
     * @throws IOException
     */
    public void updateOrCreateGeoIpData(final Datasource datasource, final Runnable renewLock) throws IOException {
        URL url = new URL(datasource.getEndpoint());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(url);

        if (shouldUpdate(datasource, manifest) == false) {
            log.info("Skipping GeoIP database update. Update is not required for {}", datasource.getName());
            datasource.getUpdateStats().setLastSkippedAt(Instant.now());
            datasourceDao.updateDatasource(datasource);
            return;
        }

        Instant startTime = Instant.now();
        String indexName = setupIndex(datasource);
        String[] header;
        List<String> fieldsToStore;
        try (CSVParser reader = geoIpDataDao.getDatabaseReader(manifest)) {
            CSVRecord headerLine = reader.iterator().next();
            header = validateHeader(headerLine).values();
            fieldsToStore = Arrays.asList(header).subList(1, header.length);
            if (datasource.isCompatible(fieldsToStore) == false) {
                throw new OpenSearchException(
                    "new fields [{}] does not contain all old fields [{}]",
                    fieldsToStore.toString(),
                    datasource.getDatabase().getFields().toString()
                );
            }
            geoIpDataDao.putGeoIpData(indexName, header, reader.iterator(), renewLock);
        }

        Instant endTime = Instant.now();
        updateDatasourceAsSucceeded(indexName, datasource, manifest, fieldsToStore, startTime, endTime);
    }

    /**
     * Return header fields of geo data with given url of a manifest file
     *
     * The first column is ip range field regardless its header name.
     * Therefore, we don't store the first column's header name.
     *
     * @param manifestUrl the url of a manifest file
     * @return header fields of geo data
     */
    public List<String> getHeaderFields(String manifestUrl) throws IOException {
        URL url = new URL(manifestUrl);
        DatasourceManifest manifest = DatasourceManifest.Builder.build(url);

        try (CSVParser reader = geoIpDataDao.getDatabaseReader(manifest)) {
            String[] fields = reader.iterator().next().values();
            return Arrays.asList(fields).subList(1, fields.length);
        }
    }

    /**
     * Delete all indices except the one which are being used
     *
     * @param datasource
     */
    public void deleteUnusedIndices(final Datasource datasource) {
        try {
            List<String> indicesToDelete = datasource.getIndices()
                .stream()
                .filter(index -> index.equals(datasource.currentIndexName()) == false)
                .collect(Collectors.toList());

            List<String> deletedIndices = deleteIndices(indicesToDelete);

            if (deletedIndices.isEmpty() == false) {
                datasource.getIndices().removeAll(deletedIndices);
                datasourceDao.updateDatasource(datasource);
            }
        } catch (Exception e) {
            log.error("Failed to delete old indices for {}", datasource.getName(), e);
        }
    }

    /**
     * Update datasource with given systemSchedule and task
     *
     * @param datasource datasource to update
     * @param systemSchedule new system schedule value
     * @param task new task value
     */
    public void updateDatasource(final Datasource datasource, final IntervalSchedule systemSchedule, final DatasourceTask task) {
        boolean updated = false;
        if (datasource.getSystemSchedule().equals(systemSchedule) == false) {
            datasource.setSystemSchedule(systemSchedule);
            updated = true;
        }
        if (datasource.getTask().equals(task) == false) {
            datasource.setTask(task);
            updated = true;
        }

        if (updated) {
            datasourceDao.updateDatasource(datasource);
        }
    }

    private List<String> deleteIndices(final List<String> indicesToDelete) {
        List<String> deletedIndices = new ArrayList<>(indicesToDelete.size());
        for (String index : indicesToDelete) {
            if (clusterService.state().metadata().hasIndex(index) == false) {
                deletedIndices.add(index);
                continue;
            }

            try {
                geoIpDataDao.deleteIp2GeoDataIndex(index);
                deletedIndices.add(index);
            } catch (Exception e) {
                log.error("Failed to delete an index [{}]", index, e);
            }
        }
        return deletedIndices;
    }

    /**
     * Validate header
     *
     * 1. header should not be null
     * 2. the number of values in header should be more than one
     *
     * @param header the header
     * @return CSVRecord the input header
     */
    private CSVRecord validateHeader(CSVRecord header) {
        if (header == null) {
            throw new OpenSearchException("geoip database is empty");
        }
        if (header.values().length < 2) {
            throw new OpenSearchException("geoip database should have at least two fields");
        }
        return header;
    }

    /***
     * Update datasource as succeeded
     *
     * @param manifest the manifest
     * @param datasource the datasource
     */
    private void updateDatasourceAsSucceeded(
        final String newIndexName,
        final Datasource datasource,
        final DatasourceManifest manifest,
        final List<String> fields,
        final Instant startTime,
        final Instant endTime
    ) {
        datasource.setCurrentIndex(newIndexName);
        datasource.setDatabase(manifest, fields);
        datasource.getUpdateStats().setLastSucceededAt(endTime);
        datasource.getUpdateStats().setLastProcessingTimeInMillis(endTime.toEpochMilli() - startTime.toEpochMilli());
        datasource.enable();
        datasource.setState(DatasourceState.AVAILABLE);
        datasourceDao.updateDatasource(datasource);
        log.info(
            "GeoIP database creation succeeded for {} and took {} seconds",
            datasource.getName(),
            Duration.between(startTime, endTime)
        );
    }

    /***
     * Setup index to add a new geoip data
     *
     * @param datasource the datasource
     * @return new index name
     */
    private String setupIndex(final Datasource datasource) {
        String indexName = datasource.newIndexName(UUID.randomUUID().toString());
        datasource.getIndices().add(indexName);
        datasourceDao.updateDatasource(datasource);
        geoIpDataDao.createIndexIfNotExists(indexName);
        return indexName;
    }

    /**
     * Determine if update is needed or not
     *
     * Update is needed when all following conditions are met
     * 1. updatedAt value in datasource is equal or before updateAt value in manifest
     * 2. SHA256 hash value in datasource is different with SHA256 hash value in manifest
     *
     * @param datasource
     * @param manifest
     * @return
     */
    private boolean shouldUpdate(final Datasource datasource, final DatasourceManifest manifest) {
        if (datasource.getDatabase().getUpdatedAt() != null
            && datasource.getDatabase().getUpdatedAt().toEpochMilli() > manifest.getUpdatedAt()) {
            return false;
        }

        if (manifest.getSha256Hash().equals(datasource.getDatabase().getSha256Hash())) {
            return false;
        }
        return true;
    }
}
