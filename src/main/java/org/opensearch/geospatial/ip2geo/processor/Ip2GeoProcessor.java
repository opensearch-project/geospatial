/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.geospatial.ip2geo.processor;

import static org.opensearch.cluster.service.ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME;
import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalList;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.ip2geo.common.DatasourceHelper;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataHelper;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
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
    private static final String PROPERTY_IP = "ip";
    private final String field;
    private final String targetField;
    private final String datasourceName;
    private final Set<String> properties;
    private final boolean ignoreMissing;
    private final boolean firstOnly;
    private final Ip2GeoCache cache;
    private final Client client;
    private final ClusterService clusterService;

    private int maxBundleSize;
    private int maxConcurrentSearches;

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
     * @param firstOnly      true if only first result should be returned in case of array
     * @param cache          the Ip2Geo cache
     * @param client         the client
     * @param clusterService the cluster service
     */
    public Ip2GeoProcessor(
        final String tag,
        final String description,
        final String field,
        final String targetField,
        final String datasourceName,
        final Set<String> properties,
        final boolean ignoreMissing,
        final boolean firstOnly,
        final Ip2GeoCache cache,
        final Client client,
        final ClusterService clusterService
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.datasourceName = datasourceName;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
        this.firstOnly = firstOnly;
        this.cache = cache;
        this.client = client;
        this.clusterService = clusterService;

        maxBundleSize = clusterService.getClusterSettings().get(Ip2GeoSettings.MAX_BUNDLE_SIZE);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(Ip2GeoSettings.MAX_BUNDLE_SIZE, newValue -> maxBundleSize = newValue);
        maxConcurrentSearches = clusterService.getClusterSettings().get(Ip2GeoSettings.MAX_CONCURRENT_SEARCHES);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(Ip2GeoSettings.MAX_CONCURRENT_SEARCHES, newValue -> maxConcurrentSearches = newValue);
    }

    /**
     * Add geo data of a given ip address to ingestDocument in asynchronous way
     *
     * @param ingestDocument the document
     * @param handler the handler
     */
    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        Object ip = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

        if (ip == null && ignoreMissing) {
            handler.accept(ingestDocument, null);
            return;
        } else if (ip == null) {
            handler.accept(null, new IllegalArgumentException("field [" + field + "] is null, cannot extract geo information."));
            return;
        }

        if (ip instanceof String) {
            executeInternal(ingestDocument, handler, (String) ip);
        } else if (ip instanceof List) {
            executeInternal(ingestDocument, handler, ((List<?>) ip));
        } else {
            throw new IllegalArgumentException("field [" + field + "] should contain only string or array of strings");
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

    /**
     * Handle single ip
     *
     * @param ingestDocument the document
     * @param handler the handler
     * @param ip the ip
     */
    private void executeInternal(
        final IngestDocument ingestDocument,
        final BiConsumer<IngestDocument, Exception> handler,
        final String ip
    ) {
        Map<String, Object> geoData = cache.get(ip, datasourceName);
        if (geoData != null) {
            if (!geoData.isEmpty()) {
                ingestDocument.setFieldValue(targetField, filteredGeoData(geoData, ip));
            }
            handler.accept(ingestDocument, null);
            return;
        }

        DatasourceHelper.getDatasource(client, datasourceName, new ActionListener<>() {
            @Override
            public void onResponse(final Datasource datasource) {
                if (datasource == null) {
                    handler.accept(null, new IllegalStateException("datasource does not exist"));
                    return;
                }

                if (datasource.isExpired()) {
                    ingestDocument.setFieldValue(targetField, DATA_EXPIRED);
                    handler.accept(ingestDocument, null);
                    return;
                }

                GeoIpDataHelper.getGeoData(client, datasource.currentIndexName(), ip, new ActionListener<>() {
                    @Override
                    public void onResponse(final Map<String, Object> stringObjectMap) {
                        cache.put(ip, datasourceName, stringObjectMap);
                        if (!stringObjectMap.isEmpty()) {
                            ingestDocument.setFieldValue(targetField, filteredGeoData(stringObjectMap, ip));
                        }
                        handler.accept(ingestDocument, null);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        handler.accept(null, e);
                    }
                });
            }

            @Override
            public void onFailure(final Exception e) {
                handler.accept(null, e);
            }
        });
    }

    /**
     * Handle multiple ips
     *
     * @param ingestDocument the document
     * @param handler the handler
     * @param ips the ip list
     */
    private void executeInternal(
        final IngestDocument ingestDocument,
        final BiConsumer<IngestDocument, Exception> handler,
        final List<?> ips
    ) {
        Map<String, Map<String, Object>> data = new HashMap<>();
        for (Object ip : ips) {
            if (ip instanceof String == false) {
                throw new IllegalArgumentException("array in field [" + field + "] should only contain strings");
            }
            String ipAddr = (String) ip;
            data.put(ipAddr, cache.get(ipAddr, datasourceName));
        }
        List<String> ipList = (List<String>) ips;
        DatasourceHelper.getDatasource(client, datasourceName, new ActionListener<>() {
            @Override
            public void onResponse(final Datasource datasource) {
                if (datasource == null) {
                    handler.accept(null, new IllegalStateException("datasource does not exist"));
                    return;
                }

                if (datasource.isExpired()) {
                    ingestDocument.setFieldValue(targetField, DATA_EXPIRED);
                    handler.accept(ingestDocument, null);
                    return;
                }
                GeoIpDataHelper.getGeoData(
                    client,
                    datasource.currentIndexName(),
                    ipList.iterator(),
                    maxBundleSize,
                    maxConcurrentSearches,
                    firstOnly,
                    data,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(final Object obj) {
                            for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
                                cache.put(entry.getKey(), datasourceName, entry.getValue());
                            }

                            if (firstOnly) {
                                for (String ipAddr : ipList) {
                                    Map<String, Object> geoData = data.get(ipAddr);
                                    // GeoData for ipAddr won't be null
                                    if (!geoData.isEmpty()) {
                                        ingestDocument.setFieldValue(targetField, geoData);
                                        handler.accept(ingestDocument, null);
                                        return;
                                    }
                                }
                                handler.accept(ingestDocument, null);
                            } else {
                                boolean match = false;
                                List<Map<String, Object>> geoDataList = new ArrayList<>(ipList.size());
                                for (String ipAddr : ipList) {
                                    Map<String, Object> geoData = data.get(ipAddr);
                                    // GeoData for ipAddr won't be null
                                    geoDataList.add(geoData.isEmpty() ? null : geoData);
                                    if (!geoData.isEmpty()) {
                                        match = true;
                                    }
                                }
                                if (match) {
                                    ingestDocument.setFieldValue(targetField, geoDataList);
                                }
                                handler.accept(ingestDocument, null);
                            }
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            handler.accept(null, e);
                        }
                    }
                );
            }

            @Override
            public void onFailure(final Exception e) {
                handler.accept(null, e);
            }
        });
    }

    private Map<String, Object> filteredGeoData(final Map<String, Object> geoData, final String ip) {
        Map<String, Object> filteredGeoData;
        if (properties == null) {
            filteredGeoData = geoData;
        } else {
            filteredGeoData = new HashMap<>();
            for (String property : this.properties) {
                if (property.equals(PROPERTY_IP)) {
                    filteredGeoData.put(PROPERTY_IP, ip);
                } else {
                    filteredGeoData.put(property, geoData.get(property));
                }
            }
        }
        return filteredGeoData;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Ip2Geo processor factory
     */
    public static final class Factory implements Processor.Factory {
        private final Ip2GeoCache cache;
        private final Client client;
        private final IngestService ingestService;
        private TimeValue timeout;

        /**
         * Default constructor
         *
         * @param cache the cache
         * @param client the client
         * @param ingestService the ingest service
         */
        public Factory(final Ip2GeoCache cache, final Client client, final IngestService ingestService) {
            this.cache = cache;
            this.client = client;
            this.ingestService = ingestService;

            timeout = Ip2GeoSettings.TIMEOUT_IN_SECONDS.get(client.settings());
            ClusterSettings clusterSettings = ingestService.getClusterService().getClusterSettings();
            clusterSettings.addSettingsUpdateConsumer(Ip2GeoSettings.TIMEOUT_IN_SECONDS, newValue -> timeout = newValue);
        }

        /**
         * When a user create a processor, this method is called twice. Once to validate the new processor and another
         * to apply cluster state change after the processor is added.
         *
         * The second call is made by ClusterApplierService. Therefore, we cannot access cluster state in the call.
         * That means, we cannot even query an index inside the call.
         *
         * Because the processor is validated in the first call, we skip the validation in the second call.
         *
         * @see org.opensearch.cluster.service.ClusterApplierService#state()
         */
        @Override
        public Ip2GeoProcessor create(
            final Map<String, Processor.Factory> registry,
            final String processorTag,
            final String description,
            final Map<String, Object> config
        ) throws IOException {
            String ipField = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "ip2geo");
            String datasourceName = readStringProperty(TYPE, processorTag, config, "datasource");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean firstOnly = readBooleanProperty(TYPE, processorTag, config, "first_only", true);

            // Skip validation for the call by cluster applier service
            if (!Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME)) {
                validate(processorTag, datasourceName, propertyNames);
            }

            return new Ip2GeoProcessor(
                processorTag,
                description,
                ipField,
                targetField,
                datasourceName,
                propertyNames == null ? null : new HashSet<>(propertyNames),
                ignoreMissing,
                firstOnly,
                cache,
                client,
                ingestService.getClusterService()
            );
        }

        private void validate(final String processorTag, final String datasourceName, final List<String> propertyNames) throws IOException {
            Datasource datasource = DatasourceHelper.getDatasource(client, datasourceName, timeout);

            if (datasource == null) {
                throw newConfigurationException(TYPE, processorTag, "datasource", "datasource [" + datasourceName + "] doesn't exist");
            }

            if (!DatasourceState.AVAILABLE.equals(datasource.getState())) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "datasource",
                    "datasource [" + datasourceName + "] is not in an available state"
                );
            }

            if (propertyNames == null) {
                return;
            }

            // Validate properties are valid. If not add all available properties.
            final Set<String> availableProperties = new HashSet<>(datasource.getDatabase().getFields());
            availableProperties.add(PROPERTY_IP);
            for (String fieldName : propertyNames) {
                if (!availableProperties.contains(fieldName)) {
                    throw newConfigurationException(
                        TYPE,
                        processorTag,
                        "properties",
                        "property [" + fieldName + "] is not available in the datasource [" + datasourceName + "]"
                    );
                }
            }
        }
    }
}
