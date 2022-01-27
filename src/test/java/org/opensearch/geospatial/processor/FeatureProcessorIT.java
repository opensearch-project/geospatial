/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.processor;

import static org.opensearch.geospatial.GeospatialObjectBuilder.GEOMETRY_TYPE_KEY;
import static org.opensearch.geospatial.GeospatialObjectBuilder.buildGeoJSONFeature;
import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.getRandomGeometryLineString;
import static org.opensearch.ingest.RandomDocumentPicks.randomString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.rest.RestStatus;

public class FeatureProcessorIT extends GeospatialRestTestCase {

    public static final int LINESTRING_TOTAL_POINTS = 4;
    public static final int LINESTRING_POINT_DIMENSION = 2;

    private double[][] randomDoubleArray(int row, int col) {
        double[][] randomArray = new double[row][col];
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                randomArray[i][j] = randomDouble();
            }
        }
        return randomArray;
    }

    public void testProcessorAvailable() throws IOException {
        String nodeIngestURL = String.join("/", "_nodes", "ingest");
        String endpoint = nodeIngestURL + "?filter_path=nodes.*.ingest.processors&pretty";
        Request request = new Request("GET", endpoint);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);
        assertTrue(responseBody.contains(FeatureProcessor.TYPE));
    }

    public void testIndexGeoJSONSuccess() throws IOException {

        String indexName = randomString(random()).toLowerCase(Locale.getDefault());
        String geoShapeField = randomString(random());
        String pipelineName = randomString(random());

        Map<String, String> geoFields = new HashMap<>();
        geoFields.put(geoShapeField, "geo_shape");

        Map<String, String> processorProperties = new HashMap<>();
        processorProperties.put(FeatureProcessor.FIELD_KEY, geoShapeField);
        Map<String, Object> geoJSONProcessorConfig = buildGeoJSONFeatureProcessorConfig(processorProperties);
        List<Map<String, Object>> configs = new ArrayList<>();
        configs.add(geoJSONProcessorConfig);

        createPipeline(pipelineName, Optional.empty(), configs);

        createIndex(indexName, Settings.EMPTY, geoFields);

        Map<String, Object> properties = new HashMap<>();
        properties.put(randomString(random()), randomString(random()));
        properties.put(randomString(random()), randomString(random()));

        JSONObject feature = buildGeoJSONFeature(
            getRandomGeometryLineString(LINESTRING_TOTAL_POINTS, LINESTRING_POINT_DIMENSION),
            buildProperties(properties)
        );
        String requestBody = feature.toString();
        Map<String, String> params = new HashMap<>();
        params.put("pipeline", pipelineName);

        String docID = randomString(random());
        indexDocument(indexName, docID, requestBody, params);

        Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull(document);

        for (Map.Entry<String, Object> property : properties.entrySet()) {
            assertEquals(document.get(property.getKey()), property.getValue());
        }

        Map<String, Object> geoShapeFieldValue = (Map<String, Object>) document.get(geoShapeField);
        assertEquals(geoShapeFieldValue.get(GEOMETRY_TYPE_KEY), GeoShapeType.LINESTRING.shapeName());

        deletePipeline(pipelineName);
        deleteIndex(indexName);

    }
}
