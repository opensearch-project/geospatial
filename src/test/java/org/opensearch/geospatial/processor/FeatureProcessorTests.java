/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.processor;

import static org.opensearch.geospatial.GeospatialRestTestCase.GEOMETRY_COORDINATES_KEY;
import static org.opensearch.geospatial.GeospatialRestTestCase.GEOMETRY_TYPE_KEY;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

public class FeatureProcessorTests extends OpenSearchTestCase {

    private Map<String, Object> buildGeoJSON(String type) {
        Map<String, Object> geoJSON = new HashMap<>();
        geoJSON.put(Feature.TYPE_KEY, type);

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "Dinagat Islands");
        geoJSON.put("properties", properties);

        Map<String, Object> geometry = new HashMap<>();
        geometry.put(GEOMETRY_TYPE_KEY, GeoShapeType.POINT.shapeName());
        geometry.put(GEOMETRY_COORDINATES_KEY, "[125.6, 10.1]");
        geoJSON.put(Feature.GEOMETRY_KEY, geometry);

        return geoJSON;

    }

    public void testGeoJSONProcessorSuccess() {
        Map<String, Object> document = buildGeoJSON(Feature.TYPE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        processor.execute(ingestDocument);
        Map<String, Object> location = (Map<String, Object>) ingestDocument.getFieldValue("location", Object.class);
        assertNotNull(location);
        assertEquals(document.get(Feature.GEOMETRY_KEY), location);
        assertEquals("Dinagat Islands", ingestDocument.getSourceAndMetadata().get("name"));
        assertNull(ingestDocument.getSourceAndMetadata().get(Feature.GEOMETRY_KEY));
        assertNull(ingestDocument.getSourceAndMetadata().get(Feature.TYPE_KEY));
        assertNull(ingestDocument.getSourceAndMetadata().get(Feature.PROPERTIES_KEY));
    }

    public void testGeoJSONProcessorUnSupportedType() {
        Map<String, Object> document = buildGeoJSON("FeatureCollection");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains("Only type Feature is supported"));
    }

    public void testGeoJSONProcessorTypeNotFound() {
        Map<String, Object> document = buildGeoJSON(Feature.TYPE);
        document.remove(Feature.TYPE_KEY);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains("type cannot be null"));
    }
}
