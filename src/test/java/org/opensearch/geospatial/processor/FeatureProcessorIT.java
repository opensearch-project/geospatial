/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.processor;

import static org.opensearch.geospatial.GeospatialObjectBuilder.GEOMETRY_TYPE_KEY;
import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.geospatial.GeospatialRestTestCase;

public class FeatureProcessorIT extends GeospatialRestTestCase {

    public void testProcessorAvailable() throws Exception {
        String nodeIngestURL = String.join("/", "_nodes", "ingest");
        String endpoint = nodeIngestURL + "?filter_path=nodes.*.ingest.processors&pretty";
        Request request = new Request("GET", endpoint);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);
        assertTrue(responseBody.contains(FeatureProcessor.TYPE));
    }

    public void testIndexGeoJSONSuccess() throws Exception {

        String indexName = randomLowerCaseString();
        String geoShapeField = randomLowerCaseString();
        String pipelineName = randomLowerCaseString();

        Map<String, String> geoFields = new HashMap<>();
        geoFields.put(geoShapeField, "geo_shape");

        Map<String, Object> processorProperties = new HashMap<>();
        processorProperties.put(FeatureProcessor.FIELD_KEY, geoShapeField);
        Map<String, Object> geoJSONProcessorConfig = buildProcessorConfig(FeatureProcessor.TYPE, processorProperties);
        List<Map<String, Object>> configs = new ArrayList<>();
        configs.add(geoJSONProcessorConfig);

        createPipeline(pipelineName, Optional.empty(), configs);

        createIndex(indexName, Settings.EMPTY, geoFields);

        Map<String, Object> properties = new HashMap<>();
        properties.put(randomLowerCaseString(), randomLowerCaseString());
        properties.put(randomLowerCaseString(), randomLowerCaseString());

        JSONObject feature = randomGeoJSONFeature(buildProperties(properties));
        String requestBody = feature.toString();
        Map<String, String> params = new HashMap<>();
        params.put("pipeline", pipelineName);

        String docID = randomLowerCaseString();
        indexDocument(indexName, docID, requestBody, params);

        Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull(document);

        for (Map.Entry<String, Object> property : properties.entrySet()) {
            assertEquals(document.get(property.getKey()), property.getValue());
        }

        Map<String, Object> geoShapeFieldValue = (Map<String, Object>) document.get(geoShapeField);
        assertNotNull(geoShapeFieldValue);
        assertNotNull(geoShapeFieldValue.get(GEOMETRY_TYPE_KEY));

        deletePipeline(pipelineName);
        deleteIndex(indexName);

    }
}
