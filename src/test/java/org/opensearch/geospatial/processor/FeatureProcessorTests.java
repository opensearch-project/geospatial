/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.processor;

import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

public class FeatureProcessorTests extends OpenSearchTestCase {

    private Map<String, Object> buildTestFeature() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "Dinagat Islands");
        return randomGeoJSONFeature(buildProperties(properties)).toMap();
    }

    public void testCreateFeatureProcessor() {
        FeatureProcessor.Factory factory = new FeatureProcessor.Factory();
        Map<String, Object> processorProperties = new HashMap<>();
        processorProperties.put(FeatureProcessor.FIELD_KEY, "location");
        Processor featureProcessor = factory.create(Collections.emptyMap(), "unit-test", "description", processorProperties);
        assertNotNull(featureProcessor);
        assertEquals(featureProcessor.getType(), FeatureProcessor.TYPE);
    }

    public void testFeatureProcessor() {
        Map<String, Object> document = buildTestFeature();
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

    public void testFeatureProcessorWithoutProperties() {
        Map<String, Object> document = buildTestFeature();
        document.remove(Feature.PROPERTIES_KEY);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        processor.execute(ingestDocument);
        Map<String, Object> location = (Map<String, Object>) ingestDocument.getFieldValue("location", Object.class);
        assertNotNull(location);
        assertEquals(document.get(Feature.GEOMETRY_KEY), location);
        assertNull(ingestDocument.getSourceAndMetadata().get(Feature.GEOMETRY_KEY));
        assertNull(ingestDocument.getSourceAndMetadata().get(Feature.TYPE_KEY));
    }

    public void testFeatureProcessorWithInvalidProperties() {
        Map<String, Object> document = buildTestFeature();
        document.put(Feature.PROPERTIES_KEY, "invalid-value");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains(Feature.PROPERTIES_KEY + " is not an instance of type Map"));
    }

    public void testFeatureProcessorUnSupportedType() {
        Map<String, Object> document = new HashMap<>();
        document.put("type", "FeatureCollection");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains("Only type Feature is supported"));
    }

    public void testFeatureProcessorTypeNotFound() {
        Map<String, Object> document = buildTestFeature();
        document.remove(Feature.TYPE_KEY);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains("type cannot be null"));
    }

    public void testFeatureProcessorWithoutGeometry() {
        Map<String, Object> document = buildTestFeature();
        document.remove(Feature.GEOMETRY_KEY);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains(Feature.GEOMETRY_KEY + " cannot be null"));
    }

    public void testFeatureProcessorWithInvalidGeometry() {
        Map<String, Object> document = buildTestFeature();
        document.put(Feature.GEOMETRY_KEY, "invalid-value");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        FeatureProcessor processor = new FeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains(Feature.GEOMETRY_KEY + " is not an instance of type Map"));
    }
}
