/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import static java.util.stream.Collectors.joining;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseStringWithSuffix;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.ACCEPTED_INDEX_SUFFIX_PATH;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.FIELD_DATA;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;
import static org.opensearch.index.query.AbstractGeometryQueryBuilder.DEFAULT_SHAPE_FIELD_NAME;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.geometry.Geometry;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.geospatial.index.query.xyshape.XYShapeQueryBuilder;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.ingest.Pipeline;
import org.opensearch.rest.RestStatus;

public abstract class GeospatialRestTestCase extends OpenSearchSecureRestTestCase {

    public static final String SOURCE = "_source";
    public static final String DOC = "_doc";
    public static final String URL_DELIMITER = "/";
    public static final String FIELD_TYPE_KEY = "type";
    public static final String MAPPING_PROPERTIES_KEY = "properties";
    public static final String MAPPING = "_mapping";
    public static final String FIELD_MAPPINGS_KEY = "mappings";
    public static final String COUNT = "_count";
    public static final String FIELD_COUNT_KEY = "count";
    public static final String PARAM_REFRESH_KEY = "refresh";
    public static final String SEARCH = "_search";
    public static final String SHAPE_RELATION = "relation";
    public static final String INDEXED_SHAPE_FIELD = "indexed_shape";
    public static final String SHAPE_INDEX_FIELD = "index";
    public static final String SHAPE_ID_FIELD = "id";
    public static final String SHAPE_INDEX_PATH_FIELD = "path";

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

    public static String indexDocument(String indexName, String body) throws IOException {
        return indexDocument(indexName, body, Map.of());
    }

    public static String indexDocument(String indexName, String body, Map<String, String> params) throws IOException {
        return indexDocument(indexName, UUIDs.randomBase64UUID(), body, params);
    }

    public static String indexDocument(String indexName, String docID, String body) throws IOException {
        return indexDocument(indexName, docID, body, Map.of());
    }

    public static String indexDocument(String indexName, String docID, String body, Map<String, String> params) throws IOException {
        String path = String.join(URL_DELIMITER, indexName, DOC, docID);
        Map<String, String> indexParams = new HashMap<>();
        if (params != null) {
            indexParams.putAll(params);
        }
        indexParams.put(PARAM_REFRESH_KEY, Boolean.TRUE.toString());
        String queryParams = indexParams.entrySet().stream().map(Object::toString).collect(joining("&"));
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(path);
        endpoint.append("?");
        endpoint.append(queryParams);

        Request request = new Request("PUT", endpoint.toString());
        request.setJsonEntity(body);
        client().performRequest(request);
        return docID;
    }

    protected Map<String, Object> buildGeoJSONFeatureProcessorConfig(Map<String, String> properties) {
        Map<String, Object> featureProcessor = new HashMap<>();
        featureProcessor.put(FeatureProcessor.TYPE, properties);
        return featureProcessor;
    }

    public Map<String, Object> getDocument(String docID, String indexName) throws Exception {
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
        String indexName = Strings.hasText(index) ? index : randomLowerCaseStringWithSuffix(ACCEPTED_INDEX_SUFFIX_PATH);
        String fieldName = Strings.hasText(geoFieldName) ? geoFieldName : randomLowerCaseString();
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), indexName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), fieldName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName(), "geo_shape");
        JSONArray values = new JSONArray();
        IntStream.range(0, totalGeoJSONObject).forEach(unUsed -> values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap()))));
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents;
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
    protected Map<String, Object> getIndexMapping(String index) throws Exception {
        String indexMappingURL = String.join(URL_DELIMITER, index, MAPPING);
        Request request = new Request("GET", indexMappingURL);
        Response response = client().performRequest(request);
        assertEquals("failed to get index mapping", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        MatcherAssert.assertThat(index + " does not exist", responseMap, Matchers.hasKey(index));
        return (Map<String, Object>) ((Map<String, Object>) responseMap.get(index)).get(FIELD_MAPPINGS_KEY);
    }

    /*
        Get index mapping's properties as map
     */
    protected Map<String, Object> getIndexProperties(String index) throws Exception {
        final Map<String, Object> indexMapping = getIndexMapping(index);
        MatcherAssert.assertThat("No properties found for index: " + index, indexMapping, Matchers.hasKey(MAPPING_PROPERTIES_KEY));
        return (Map<String, Object>) indexMapping.get(MAPPING_PROPERTIES_KEY);
    }

    protected int getIndexDocumentCount(String index) throws Exception {
        String indexDocumentCountPath = String.join(URL_DELIMITER, index, COUNT);
        Request request = new Request("GET", indexDocumentCountPath);
        Response response = client().performRequest(request);
        assertEquals("failed to get index document count", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        MatcherAssert.assertThat(FIELD_COUNT_KEY + " does not exist", responseMap, Matchers.hasKey(FIELD_COUNT_KEY));
        return (Integer) responseMap.get(FIELD_COUNT_KEY);
    }

    private Response uploadGeoJSONFeaturesByMethod(String method, int featureCount, String indexName, String geospatialFieldName)
        throws IOException {
        // upload geoJSON
        String path = String.join(
            URL_DELIMITER,
            getPluginURLPrefix(),
            RestUploadGeoJSONAction.ACTION_OBJECT,
            RestUploadGeoJSONAction.ACTION_UPLOAD
        );
        Request request = new Request(method, path);
        final JSONObject requestBody = buildUploadGeoJSONRequestContent(featureCount, indexName, geospatialFieldName);
        request.setJsonEntity(requestBody.toString());
        return client().performRequest(request);
    }

    protected final Response uploadGeoJSONFeaturesIntoExistingIndex(int featureCount, String indexName, String geospatialFieldName)
        throws IOException {
        return uploadGeoJSONFeaturesByMethod("PUT", featureCount, indexName, geospatialFieldName);
    }

    protected final Response uploadGeoJSONFeatures(int featureCount, String indexName, String geospatialFieldName) throws IOException {
        return uploadGeoJSONFeaturesByMethod("POST", featureCount, indexName, geospatialFieldName);
    }

    public String buildContentAsString(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return Strings.toString(builder);
    }

    public String buildSearchBodyAsString(
        CheckedConsumer<XContentBuilder, IOException> searchQueryBuilder,
        String queryType,
        String fieldName
    ) throws IOException {
        return buildContentAsString(builder -> {
            builder.startObject("query").startObject(queryType).startObject(fieldName);
            searchQueryBuilder.accept(builder);
            builder.endObject();
            builder.endObject().endObject();
        });
    }

    public SearchResponse searchIndex(String indexName, String entity) throws Exception {
        String path = String.join(URL_DELIMITER, indexName, SEARCH);
        final Request request = new Request("GET", path);
        request.setJsonEntity(entity);
        final Response response = client().performRequest(request);
        return SearchResponse.fromXContent(createParser(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity())));
    }

    protected String buildDocumentWithWKT(String fieldName, String wktFormat) throws IOException {
        final String document = buildContentAsString(
            builder -> builder.field(randomLowerCaseString(), randomLowerCaseString()).field(fieldName, wktFormat)
        );
        return document;
    }

    protected String buildDocumentWithGeoJSON(String fieldName, Geometry geometry) throws IOException {
        final String document = buildContentAsString(builder -> {
            builder.field(randomLowerCaseString(), randomLowerCaseString()).field(fieldName);
            GeoJson.toXContent(geometry, builder, EMPTY_PARAMS);
        });
        return document;
    }

    public String indexDocumentUsingWKT(String indexName, String fieldName, String wktFormat) throws IOException {
        final String document = buildDocumentWithWKT(fieldName, wktFormat);
        return indexDocument(indexName, document);
    }

    public String indexDocumentUsingGeoJSON(String indexName, String fieldName, Geometry geometry) throws IOException {
        final String document = buildDocumentWithGeoJSON(fieldName, geometry);
        return indexDocument(indexName, document);
    }

    public SearchResponse searchUsingShapeRelation(String indexName, String fieldName, Geometry geometry, ShapeRelation shapeRelation)
        throws Exception {
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(geometry, builder, EMPTY_PARAMS);
            builder.field(SHAPE_RELATION, shapeRelation.getRelationName());
        }, XYShapeQueryBuilder.NAME, fieldName);

        return searchIndex(indexName, searchEntity);
    }

    public void createIndexedShapeIndex() throws IOException {
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));
    }

    public SearchResponse searchUsingIndexedShapeIndex(
        String indexName,
        String indexedShapeIndex,
        String indexedShapePath,
        String docId,
        String fieldName
    ) throws Exception {
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.startObject(INDEXED_SHAPE_FIELD);
            builder.field(SHAPE_INDEX_FIELD, indexedShapeIndex);
            builder.field(SHAPE_ID_FIELD, docId);
            builder.field(SHAPE_INDEX_PATH_FIELD, indexedShapePath);
            builder.endObject();
        }, XYShapeQueryBuilder.NAME, fieldName);

        return searchIndex(indexName, searchEntity);
    }

}
