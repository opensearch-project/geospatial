/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.geospatial.GeospatialParser;

/**
 * ContentBuilder is responsible for preparing Request that can be executed
 * to upload GeoJSON Features as Documents.
 */
public class ContentBuilder {
    public static final String GEOJSON_FEATURE_ID_FIELD = "id";
    public static final String DOCUMENT_TYPE = "_doc";
    private final Client client;

    public ContentBuilder(Client client) {
        this.client = Objects.requireNonNull(client, "Client cannot be null");
    }

    public Optional<BulkRequestBuilder> prepare(UploadGeoJSONRequestContent content, String pipeline) {
        return prepareContentRequest(content, pipeline);
    }

    private BulkRequestBuilder prepareBulkRequestBuilder() {
        return client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    }

    // build BulkRequestBuilder, by, iterating, UploadGeoJSONRequestContent's data. This depends on
    // GeospatialParser.getFeatures to extract features from user input, create IndexRequestBuilder
    // with index name and pipeline.
    private Optional<BulkRequestBuilder> prepareContentRequest(UploadGeoJSONRequestContent content, String pipeline) {
        BulkRequestBuilder builder = prepareBulkRequestBuilder();
        List<Object> documents = (List<Object>) content.getData();
        documents.stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(GeospatialParser::getFeatures)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(List::stream)
            .map(documentSource -> createIndexRequestBuilder(documentSource))
            .map(indexRequestBuilder -> indexRequestBuilder.setIndex(content.getIndexName()))
            .map(indexRequestBuilder -> indexRequestBuilder.setPipeline(pipeline))
            .map(indexRequestBuilder -> indexRequestBuilder.setType(DOCUMENT_TYPE))
            .forEach(builder::add);
        if (builder.numberOfActions() < 1) { // check any features to upload
            return Optional.empty();
        }
        return Optional.of(builder);
    }

    private IndexRequestBuilder createIndexRequestBuilder(Map<String, Object> source) {
        final IndexRequestBuilder requestBuilder = client.prepareIndex().setSource(source);
        String id = GeospatialParser.extractValueAsString(source, GEOJSON_FEATURE_ID_FIELD);
        return Strings.hasText(id) ? requestBuilder.setId(id) : requestBuilder;
    }
}
