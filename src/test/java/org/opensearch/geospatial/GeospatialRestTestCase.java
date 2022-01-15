/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Pipeline;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public abstract class GeospatialRestTestCase extends OpenSearchRestTestCase {

    public static final String SOURCE = "_source";
    public static final String DOC = "_doc";
    public static final String URL_DELIMITER = "/";
    public static final String GEOMETRY_TYPE_KEY = "type";
    public static final String GEOMETRY_COORDINATES_KEY = "coordinates";

    private static String buildPipelinePath(String name) {
        return String.join(URL_DELIMITER, "_ingest", "pipeline", name);
    }

    protected static void createPipeline(String name, Optional<String> description, List<Map<String, Object>> processorConfigs) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        if (description.isPresent()) {
            builder.field(Pipeline.DESCRIPTION_KEY, description.get());
        }
        if (processorConfigs != null && !processorConfigs.isEmpty()) {
            builder.field(Pipeline.PROCESSORS_KEY, processorConfigs);
        }
        builder.endObject();

        Request request = new Request("PUT", buildPipelinePath(name));
        request.setJsonEntity(Strings.toString(builder));
        client().performRequest(request);
    }

    protected static void deletePipeline(String name) throws IOException {
        Request request = new Request("DELETE", buildPipelinePath(name));
        client().performRequest(request);
    }

    protected static void createIndex(String name, Settings settings, Map<String, String> fieldMap) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject(Feature.PROPERTIES_KEY);
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            xContentBuilder.startObject(entry.getKey())
                .field(Feature.TYPE_KEY, entry.getValue())
                .endObject();
        }
        xContentBuilder.endObject().endObject();
        String mapping = Strings.toString(xContentBuilder);
        createIndex(name, settings, mapping.substring(1, mapping.length() - 1));
    }

    public static void indexDocument(String indexName, String docID, String body, Map<String, String> params) throws IOException {

        String path = String.join(URL_DELIMITER, indexName, DOC, docID);
        String queryParams = params.entrySet().stream()
            .map(Object::toString)
            .collect(joining("&"));
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(path);
        endpoint.append("?");
        endpoint.append(queryParams);

        Request request = new Request("PUT", endpoint.toString());
        request.setJsonEntity(body);
        client().performRequest(request);
    }

    protected Map<String, Object> buildGeoJSONProcessorConfig(Map<String, String> properties) {
        Map<String, Object> geoJSONProcessor = new HashMap<>();
        geoJSONProcessor.put(FeatureProcessor.TYPE, properties);
        return geoJSONProcessor;
    }

    public String buildGeoJSONFeatureAsString(String type, Object value, Map<String, Object> properties) throws IOException {

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .field(Feature.TYPE_KEY, Feature.TYPE)
            .startObject(Feature.GEOMETRY_KEY)
            .field(GEOMETRY_TYPE_KEY, type)
            .field(GEOMETRY_COORDINATES_KEY, value)
            .endObject();
        builder.startObject(Feature.PROPERTIES_KEY);
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject().endObject();
        return Strings.toString(builder);
    }

    public Map<String, Object> getDocument(String docID, String indexName) throws IOException {
        String path = String.join(URL_DELIMITER, indexName, DOC, docID);
        final Request request = new Request("GET", path);
        final Response response = client().performRequest(request);

        final Map<String, Object> responseMap =
            createParser(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity())).map();
        if (!responseMap.containsKey(SOURCE)) {
            return null;
        }
        final Map<String, Object> docMap = (Map<String, Object>) responseMap.get(SOURCE);
        return docMap;
    }
}
