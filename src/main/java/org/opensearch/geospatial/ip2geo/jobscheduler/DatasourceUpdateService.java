/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opensearch.OpenSearchException;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataFacade;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;

@Log4j2
public class DatasourceUpdateService {
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;
    private final Client client;
    private final DatasourceFacade datasourceFacade;
    private final GeoIpDataFacade geoIpDataFacade;

    public DatasourceUpdateService(
        final ClusterService clusterService,
        final Client client,
        final DatasourceFacade datasourceFacade,
        final GeoIpDataFacade geoIpDataFacade
    ) {
        this.clusterService = clusterService;
        this.clusterSettings = clusterService.getClusterSettings();
        this.client = client;
        this.datasourceFacade = datasourceFacade;
        this.geoIpDataFacade = geoIpDataFacade;
    }

    /**
     * Update GeoIp data
     *
     * @param datasource
     * @throws Exception
     */
    public void updateOrCreateGeoIpData(final Datasource datasource) throws Exception {
        URL url = new URL(datasource.getEndpoint());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(url);

        if (shouldUpdate(datasource, manifest) == false) {
            log.info("Skipping GeoIP database update. Update is not required for {}", datasource.getName());
            datasource.getUpdateStats().setLastSkippedAt(Instant.now());
            datasourceFacade.updateDatasource(datasource);
            return;
        }

        Instant startTime = Instant.now();
        String indexName = setupIndex(manifest, datasource);
        String[] header;
        List<String> fieldsToStore;
        try (CSVParser reader = geoIpDataFacade.getDatabaseReader(manifest)) {
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
            geoIpDataFacade.putGeoIpData(indexName, header, reader.iterator(), clusterSettings.get(Ip2GeoSettings.INDEXING_BULK_SIZE));
        }

        Instant endTime = Instant.now();
        updateDatasourceAsSucceeded(datasource, manifest, fieldsToStore, startTime, endTime);
    }

    /**
     * Delete all indices except the one which are being used
     *
     * @param parameter
     */
    public void deleteUnusedIndices(final Datasource parameter) {
        try {
            List<String> indicesToDelete = parameter.getIndices()
                .stream()
                .filter(index -> index.equals(parameter.currentIndexName()) == false)
                .collect(Collectors.toList());

            List<String> deletedIndices = deleteIndices(indicesToDelete);

            if (deletedIndices.isEmpty() == false) {
                parameter.getIndices().removeAll(deletedIndices);
                datasourceFacade.updateDatasource(parameter);
            }
        } catch (Exception e) {
            log.error("Failed to delete old indices for {}", parameter.getName(), e);
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
                if (geoIpDataFacade.deleteIp2GeoDataIndex(index).isAcknowledged()) {
                    deletedIndices.add(index);
                } else {
                    log.error("Failed to delete an index [{}]", index);
                }
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
     * @return
     * @throws IOException
     */
    private void updateDatasourceAsSucceeded(
        final Datasource datasource,
        final DatasourceManifest manifest,
        final List<String> fields,
        final Instant startTime,
        final Instant endTime
    ) throws IOException {
        datasource.setDatabase(manifest, fields);
        datasource.getUpdateStats().setLastSucceededAt(endTime);
        datasource.getUpdateStats().setLastProcessingTimeInMillis(endTime.toEpochMilli() - startTime.toEpochMilli());
        datasource.enable();
        datasource.setState(DatasourceState.AVAILABLE);
        datasourceFacade.updateDatasource(datasource);
        log.info(
            "GeoIP database creation succeeded for {} and took {} seconds",
            datasource.getName(),
            Duration.between(startTime, endTime)
        );
    }

    /***
     * Setup index to add a new geoip data
     *
     * @param manifest the manifest
     * @param datasource the datasource
     * @return
     * @throws IOException
     */
    private String setupIndex(final DatasourceManifest manifest, final Datasource datasource) throws IOException {
        String indexName = datasource.indexNameFor(manifest);
        datasource.getIndices().add(indexName);
        datasourceFacade.updateDatasource(datasource);
        geoIpDataFacade.createIndexIfNotExists(indexName);
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
