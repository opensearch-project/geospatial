/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.geospatial.ip2geo.processor;

import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalList;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.dao.DatasourceDao;
import org.opensearch.geospatial.ip2geo.dao.GeoIpDataDao;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoCachedDao;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Processor;

/**
 * Ip2Geo processor
 */
@Log4j2
public final class Ip2GeoProcessor extends AbstractProcessor {
    private static final Map<String, Object> DATA_EXPIRED = Map.of("error", "ip2geo_data_expired");
    public static final String CONFIG_FIELD = "field";
    public static final String CONFIG_TARGET_FIELD = "target_field";
    public static final String CONFIG_DATASOURCE = "datasource";
    public static final String CONFIG_PROPERTIES = "properties";
    public static final String CONFIG_IGNORE_MISSING = "ignore_missing";

    private final String field;
    private final String targetField;
    /**
     * @return The datasource name
     */
    @Getter
    private final String datasourceName;
    private final Set<String> properties;
    private final boolean ignoreMissing;
    private final ClusterSettings clusterSettings;
    private final DatasourceDao datasourceDao;
    private final GeoIpDataDao geoIpDataDao;
    private final Ip2GeoCachedDao ip2GeoCachedDao;

    /**
     * Ip2Geo processor type
     */
    public static final String TYPE = "ip2geo";

    /**
     * Construct an Ip2Geo processor.
     * @param tag            the processor tag
     * @param description    the processor description
     * @param field          the source field to geo-IP map
     * @param targetField    the target field
     * @param datasourceName the datasourceName
     * @param properties     the properties
     * @param ignoreMissing  true if documents with a missing value for the field should be ignored
     * @param clusterSettings the cluster settings
     * @param datasourceDao the datasource facade
     * @param geoIpDataDao the geoip data facade
     * @param ip2GeoCachedDao the cache
     */
    public Ip2GeoProcessor(
        final String tag,
        final String description,
        final String field,
        final String targetField,
        final String datasourceName,
        final Set<String> properties,
        final boolean ignoreMissing,
        final ClusterSettings clusterSettings,
        final DatasourceDao datasourceDao,
        final GeoIpDataDao geoIpDataDao,
        final Ip2GeoCachedDao ip2GeoCachedDao
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.datasourceName = datasourceName;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
        this.clusterSettings = clusterSettings;
        this.datasourceDao = datasourceDao;
        this.geoIpDataDao = geoIpDataDao;
        this.ip2GeoCachedDao = ip2GeoCachedDao;
    }

    /**
     * Add geo data of a given ip address to ingestDocument in asynchronous way
     *
     * @param ingestDocument the document
     * @param handler the handler
     */
    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            Object ip = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

            if (ip == null) {
                handler.accept(ingestDocument, null);
                return;
            }

            if (ip instanceof String) {
                executeInternal(ingestDocument, handler, (String) ip);
            } else if (ip instanceof List) {
                executeInternal(ingestDocument, handler, ((List<?>) ip));
            } else {
                handler.accept(
                    null,
                    new IllegalArgumentException(
                        String.format(Locale.ROOT, "field [%s] should contain only string or array of strings", field)
                    )
                );
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    /**
     * Use {@code execute(IngestDocument, BiConsumer<IngestDocument, Exception>)} instead
     *
     * @param ingestDocument the document
     * @return none
     */
    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        throw new IllegalStateException("Not implemented");
    }

    @VisibleForTesting
    protected void executeInternal(
        final IngestDocument ingestDocument,
        final BiConsumer<IngestDocument, Exception> handler,
        final String ip
    ) {
        validateDatasourceIsInAvailableState(datasourceName);
        String indexName = ip2GeoCachedDao.getIndexName(datasourceName);
        if (ip2GeoCachedDao.isExpired(datasourceName) || indexName == null) {
            handleExpiredData(ingestDocument, handler);
            return;
        }

        try {
            geoIpDataDao.getGeoIpData(indexName, ip, getSingleGeoIpDataListener(ingestDocument, handler));
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    @VisibleForTesting
    protected ActionListener<Map<String, Object>> getSingleGeoIpDataListener(
        final IngestDocument ingestDocument,
        final BiConsumer<IngestDocument, Exception> handler
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(final Map<String, Object> ipToGeoData) {
                try {
                    if (ipToGeoData.isEmpty() == false) {
                        ingestDocument.setFieldValue(targetField, filteredGeoData(ipToGeoData));
                    }
                    handler.accept(ingestDocument, null);
                } catch (Exception e) {
                    handler.accept(null, e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                handler.accept(null, e);
            }
        };
    }

    private Map<String, Object> filteredGeoData(final Map<String, Object> geoData) {
        if (properties == null) {
            return geoData;
        }

        return properties.stream().filter(p -> geoData.containsKey(p)).collect(Collectors.toMap(p -> p, p -> geoData.get(p)));
    }

    private List<Map<String, Object>> filteredGeoData(final List<Map<String, Object>> geoData) {
        if (properties == null) {
            return geoData;
        }

        return geoData.stream().map(this::filteredGeoData).collect(Collectors.toList());
    }

    private void validateDatasourceIsInAvailableState(final String datasourceName) {
        if (ip2GeoCachedDao.has(datasourceName) == false) {
            throw new IllegalStateException("datasource does not exist");
        }

        if (DatasourceState.AVAILABLE.equals(ip2GeoCachedDao.getState(datasourceName)) == false) {
            throw new IllegalStateException("datasource is not in an available state");
        }
    }

    private void handleExpiredData(final IngestDocument ingestDocument, final BiConsumer<IngestDocument, Exception> handler) {
        ingestDocument.setFieldValue(targetField, DATA_EXPIRED);
        handler.accept(ingestDocument, null);
    }

    /**
     * Handle multiple ips
     *
     * @param ingestDocument the document
     * @param handler the handler
     * @param ips the ip list
     */
    @VisibleForTesting
    protected void executeInternal(
        final IngestDocument ingestDocument,
        final BiConsumer<IngestDocument, Exception> handler,
        final List<?> ips
    ) {
        for (Object ip : ips) {
            if (ip instanceof String == false) {
                throw new IllegalArgumentException("array in field [" + field + "] should only contain strings");
            }
        }

        validateDatasourceIsInAvailableState(datasourceName);
        String indexName = ip2GeoCachedDao.getIndexName(datasourceName);
        if (ip2GeoCachedDao.isExpired(datasourceName) || indexName == null) {
            handleExpiredData(ingestDocument, handler);
            return;
        }

        geoIpDataDao.getGeoIpData(indexName, (List<String>) ips, getMultiGeoIpDataListener(ingestDocument, handler));
    }

    @VisibleForTesting
    protected ActionListener<List<Map<String, Object>>> getMultiGeoIpDataListener(
        final IngestDocument ingestDocument,
        final BiConsumer<IngestDocument, Exception> handler
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(final List<Map<String, Object>> ipToGeoData) {
                try {
                    if (ipToGeoData.isEmpty() == false) {
                        ingestDocument.setFieldValue(targetField, filteredGeoData(ipToGeoData));
                    }
                    handler.accept(ingestDocument, null);
                } catch (Exception e) {
                    handler.accept(null, e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                handler.accept(null, e);
            }
        };
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Ip2Geo processor factory
     */
    @AllArgsConstructor
    public static final class Factory implements Processor.Factory {
        private final IngestService ingestService;
        private final DatasourceDao datasourceDao;
        private final GeoIpDataDao geoIpDataDao;
        private final Ip2GeoCachedDao ip2GeoCachedDao;

        /**
         * Within this method, blocking request cannot be called because this method is executed in a transport thread.
         * This means, validation using data in an index won't work.
         */
        @Override
        public Ip2GeoProcessor create(
            final Map<String, Processor.Factory> registry,
            final String processorTag,
            final String description,
            final Map<String, Object> config
        ) throws IOException {
            String ipField = readStringProperty(TYPE, processorTag, config, CONFIG_FIELD);
            String targetField = readStringProperty(TYPE, processorTag, config, CONFIG_TARGET_FIELD, "ip2geo");
            String datasourceName = readStringProperty(TYPE, processorTag, config, CONFIG_DATASOURCE);
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, CONFIG_PROPERTIES);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, CONFIG_IGNORE_MISSING, false);

            return new Ip2GeoProcessor(
                processorTag,
                description,
                ipField,
                targetField,
                datasourceName,
                propertyNames == null ? null : new HashSet<>(propertyNames),
                ignoreMissing,
                ingestService.getClusterService().getClusterSettings(),
                datasourceDao,
                geoIpDataDao,
                ip2GeoCachedDao
            );
        }
    }
}
