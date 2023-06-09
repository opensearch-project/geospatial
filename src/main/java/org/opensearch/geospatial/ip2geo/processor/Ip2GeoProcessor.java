/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.geospatial.ip2geo.processor;

import static org.opensearch.cluster.service.ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME;
import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;
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

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataFacade;
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
    private final DatasourceFacade datasourceFacade;
    private final GeoIpDataFacade geoIpDataFacade;

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
     * @param datasourceFacade the datasource facade
     * @param geoIpDataFacade the geoip data facade
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
        final DatasourceFacade datasourceFacade,
        final GeoIpDataFacade geoIpDataFacade
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.datasourceName = datasourceName;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
        this.clusterSettings = clusterSettings;
        this.datasourceFacade = datasourceFacade;
        this.geoIpDataFacade = geoIpDataFacade;
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
        datasourceFacade.getDatasource(datasourceName, new ActionListener<>() {
            @Override
            public void onResponse(final Datasource datasource) {
                if (datasource == null) {
                    handler.accept(null, new IllegalStateException("datasource does not exist"));
                    return;
                }

                String indexName = datasource.currentIndexName();
                if (indexName == null) {
                    ingestDocument.setFieldValue(targetField, DATA_EXPIRED);
                    handler.accept(ingestDocument, null);
                    return;
                }

                try {
                    geoIpDataFacade.getGeoIpData(indexName, ip, getSingleGeoIpDataListener(ingestDocument, handler));
                } catch (Exception e) {
                    handler.accept(null, e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                handler.accept(null, e);
            }
        });
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
        datasourceFacade.getDatasource(datasourceName, new ActionListener<>() {
            @Override
            public void onResponse(final Datasource datasource) {
                if (datasource == null) {
                    handler.accept(null, new IllegalStateException("datasource does not exist"));
                    return;
                }

                String indexName = datasource.currentIndexName();
                if (indexName == null) {
                    ingestDocument.setFieldValue(targetField, DATA_EXPIRED);
                    handler.accept(ingestDocument, null);
                    return;
                }

                try {
                    geoIpDataFacade.getGeoIpData(indexName, (List<String>) ips, getMultiGeoIpDataListener(ingestDocument, handler));
                } catch (Exception e) {
                    handler.accept(null, e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                handler.accept(null, e);
            }
        });
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
    public static final class Factory implements Processor.Factory {
        private final IngestService ingestService;
        private final DatasourceFacade datasourceFacade;
        private final GeoIpDataFacade geoIpDataFacade;

        /**
         * Default constructor
         *
         * @param ingestService the ingest service
         * @param datasourceFacade the datasource facade
         * @param geoIpDataFacade the geoip data facade
         */
        public Factory(final IngestService ingestService, final DatasourceFacade datasourceFacade, final GeoIpDataFacade geoIpDataFacade) {
            this.ingestService = ingestService;
            this.datasourceFacade = datasourceFacade;
            this.geoIpDataFacade = geoIpDataFacade;
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
            String ipField = readStringProperty(TYPE, processorTag, config, CONFIG_FIELD);
            String targetField = readStringProperty(TYPE, processorTag, config, CONFIG_TARGET_FIELD, "ip2geo");
            String datasourceName = readStringProperty(TYPE, processorTag, config, CONFIG_DATASOURCE);
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, CONFIG_PROPERTIES);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, CONFIG_IGNORE_MISSING, false);

            // Skip validation for the call by cluster applier service
            if (Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME) == false) {
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
                ingestService.getClusterService().getClusterSettings(),
                datasourceFacade,
                geoIpDataFacade
            );
        }

        private void validate(final String processorTag, final String datasourceName, final List<String> propertyNames) throws IOException {
            Datasource datasource = datasourceFacade.getDatasource(datasourceName);

            if (datasource == null) {
                throw newConfigurationException(TYPE, processorTag, "datasource", "datasource [" + datasourceName + "] doesn't exist");
            }

            if (DatasourceState.AVAILABLE.equals(datasource.getState()) == false) {
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
            for (String fieldName : propertyNames) {
                if (availableProperties.contains(fieldName) == false) {
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
