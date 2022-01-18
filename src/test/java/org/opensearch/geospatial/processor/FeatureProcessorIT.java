/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.processor;

import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.ingest.RandomDocumentPicks.randomString;

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
        Map<String, Object> geoJSONProcessorConfig = buildGeoJSONProcessorConfig(processorProperties);
        List<Map<String, Object>> configs = new ArrayList<>();
        configs.add(geoJSONProcessorConfig);

        createPipeline(pipelineName, Optional.empty(), configs);

        createIndex(indexName, Settings.EMPTY, geoFields);

        Map<String, Object> properties = new HashMap<>();
        properties.put(randomString(random()), randomString(random()));
        properties.put(randomString(random()), randomString(random()));


        String body = buildGeoJSONFeatureAsString(
            GeoShapeType.LINESTRING.shapeName(),
            randomDoubleArray(LINESTRING_TOTAL_POINTS, LINESTRING_POINT_DIMENSION), properties);
        Map<String, String> params = new HashMap<>();
        params.put("pipeline", pipelineName);

        String docID = randomString(random());
        indexDocument(indexName, docID, body, params);

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
