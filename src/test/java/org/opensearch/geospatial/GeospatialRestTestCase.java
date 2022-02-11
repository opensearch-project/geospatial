/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import static java.util.stream.Collectors.joining;
import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.FIELD_DATA;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.http.util.EntityUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Pipeline;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class GeospatialRestTestCase extends OpenSearchRestTestCase {

    public static final String SOURCE = "_source";
    public static final String DOC = "_doc";
    public static final String URL_DELIMITER = "/";
    public static final String FIELD_TYPE_KEY = "type";
    public static final String MAPPING_PROPERTIES_KEY = "properties";
    public static final int RANDOM_STRING_MIN_LENGTH = 2;
    public static final int RANDOM_STRING_MAX_LENGTH = 16;
    public static final String MAPPING = "_mapping";
    public static final String FIELD_MAPPINGS_KEY = "mappings";
    public static final String COUNT = "_count";
    public static final String FIELD_COUNT_KEY = "count";

    private static String buildPipelinePath(String name) {
        return String.join(URL_DELIMITER, "_ingest", "pipeline", name);
    }

    protected static void createPipeline(String name, Optional<String> description, List<Map<String, Object>> processorConfigs)
        throws IOException {
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
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject(MAPPING_PROPERTIES_KEY);
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            xContentBuilder.startObject(entry.getKey()).field(FIELD_TYPE_KEY, entry.getValue()).endObject();
        }
        xContentBuilder.endObject().endObject();
        String mapping = Strings.toString(xContentBuilder);
        createIndex(name, settings, mapping.substring(1, mapping.length() - 1));
    }

    public static void indexDocument(String indexName, String docID, String body, Map<String, String> params) throws IOException {

        String path = String.join(URL_DELIMITER, indexName, DOC, docID);
        String queryParams = params.entrySet().stream().map(Object::toString).collect(joining("&"));
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(path);
        endpoint.append("?");
        endpoint.append(queryParams);

        Request request = new Request("PUT", endpoint.toString());
        request.setJsonEntity(body);
        client().performRequest(request);
    }

    protected Map<String, Object> buildGeoJSONFeatureProcessorConfig(Map<String, String> properties) {
        Map<String, Object> featureProcessor = new HashMap<>();
        featureProcessor.put(FeatureProcessor.TYPE, properties);
        return featureProcessor;
    }

    public Map<String, Object> getDocument(String docID, String indexName) throws IOException {
        String path = String.join(URL_DELIMITER, indexName, DOC, docID);
        final Request request = new Request("GET", path);
        final Response response = client().performRequest(request);

        final Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()))
            .map();
        if (!responseMap.containsKey(SOURCE)) {
            return null;
        }
        final Map<String, Object> docMap = (Map<String, Object>) responseMap.get(SOURCE);
        return docMap;
    }

    // TODO This method is copied from unit test. Refactor to common class to share across tests
    protected JSONObject buildUploadGeoJSONRequestContent(int totalGeoJSONObject, String index, String geoFieldName) {
        JSONObject contents = new JSONObject();
        String indexName = Strings.hasText(index) ? index : randomLowerCaseString();
        String fieldName = Strings.hasText(geoFieldName) ? geoFieldName : randomLowerCaseString();
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), indexName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), fieldName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName(), "geo_shape");
        JSONArray values = new JSONArray();
        IntStream.range(0, totalGeoJSONObject).forEach(unUsed -> values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap()))));
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents;
    }

    private String randomString() {
        return randomAlphaOfLengthBetween(RANDOM_STRING_MIN_LENGTH, RANDOM_STRING_MAX_LENGTH);
    }

    public String randomLowerCaseString() {
        return randomString().toLowerCase(Locale.getDefault());
    }

    private RestStatus getIndexHeadRequestStatus(String indexName) throws IOException {
        Request headRequest = new Request("HEAD", indexName);
        Response indexResponse = client().performRequest(headRequest);
        return RestStatus.fromCode(indexResponse.getStatusLine().getStatusCode());
    }

    protected void assertIndexExists(String indexName) throws IOException {
        assertEquals("index does not exist", RestStatus.OK, getIndexHeadRequestStatus(indexName));
    }

    protected void assertIndexNotExists(String indexName) throws IOException {
        assertEquals("index already exist", RestStatus.NOT_FOUND, getIndexHeadRequestStatus(indexName));
    }

    /*
     Get index mapping as map
     */
    protected Map<String, Object> getIndexMapping(String index) throws IOException {

        String indexMappingURL = String.join(URL_DELIMITER, index, MAPPING);
        Request request = new Request("GET", indexMappingURL);
        Response response = client().performRequest(request);
        assertEquals("failed to get index mapping", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        MatcherAssert.assertThat(index + " does not exist", responseMap, Matchers.hasKey(index));
        return (Map<String, Object>) ((Map<String, Object>) responseMap.get(index)).get(FIELD_MAPPINGS_KEY);
    }

    protected int getIndexDocumentCount(String index) throws IOException {
        String indexDocumentCountPath = String.join(URL_DELIMITER, index, COUNT);
        Request request = new Request("GET", indexDocumentCountPath);
        Response response = client().performRequest(request);
        assertEquals("failed to get index document count", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        MatcherAssert.assertThat(FIELD_COUNT_KEY + " does not exist", responseMap, Matchers.hasKey(FIELD_COUNT_KEY));
        return (Integer) responseMap.get(FIELD_COUNT_KEY);
    }
}
