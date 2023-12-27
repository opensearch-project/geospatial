/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;

import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * IndexManager is responsible for managing index operations like create, delete, etc...
 */
@AllArgsConstructor
public class IndexManager {
    public static final String FIELD_TYPE_KEY = "type";
    public static final String MAPPING_PROPERTIES_KEY = "properties";
    private static final Logger LOGGER = LogManager.getLogger(IndexManager.class);
    @NonNull
    private final IndicesAdminClient client;

    /**
     * Creates an index and notifies the listener on status of action.
     * @param indexName Index Name to be created
     * @param fieldNameTypeMap Map of field name and type, that will be used for creating mapping.
     * @param createIndexStep Notification Listener that will notify status of action
     */
    public void create(final String indexName, final Map<String, String> fieldNameTypeMap, final StepListener<Void> createIndexStep) {
        try (XContentBuilder mapping = buildMapping(fieldNameTypeMap)) {
            createIndex(indexName, mapping, createIndexStep);
        } catch (IOException mappingFailedException) {
            createIndexStep.onFailure(mappingFailedException);
        }
    }

    private XContentBuilder buildMapping(Map<String, String> fieldMap) throws IOException {
        final XContentBuilder mapBuilder = XContentFactory.jsonBuilder().startObject().startObject(MAPPING_PROPERTIES_KEY);
        for (Map.Entry<String, String> field : fieldMap.entrySet()) {
            mapBuilder.startObject(field.getKey()).field(FIELD_TYPE_KEY, field.getValue()).endObject();
        }
        mapBuilder.endObject().endObject();
        return mapBuilder;
    }

    private void createIndex(String indexName, XContentBuilder mapping, StepListener<Void> createIndexStep) {
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(mapping);
        client.create(request, ActionListener.wrap(createIndexResponse -> {
            StringBuilder message = new StringBuilder("Created index: ").append(indexName);
            LOGGER.info(message.toString());
            createIndexStep.onResponse(null);
        }, createIndexStep::onFailure));
    }
}
