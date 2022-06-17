/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.processor;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.util.Map;

import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.geospatial.geojson.FeatureFactory;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

/**
 * {@link FeatureProcessor} converts GeoJSON Feature into a document by extracting properties as its own field,
 * move GeoJSON Geometry object into OpenSearch geo_shape field, and, remove fields like "type"
 */
public class FeatureProcessor extends AbstractProcessor {

    public static final String FIELD_KEY = "field";
    public static final String TYPE = "geojson-feature";
    private final String geoShapeField;

    public FeatureProcessor(String tag, String description, String geoShapeField) {
        super(tag, description);
        this.geoShapeField = geoShapeField;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        // 1. Create Feature from ingesting document
        // 2. Remove field "type", since, we are not storing as geo-json
        // 3. Move properties.* as document's fields, since, we don't have to group it inside "properties"
        // 5. Move geojson's geometry object to geoshape field.
        final Feature feature = FeatureFactory.create(ingestDocument.getSourceAndMetadata());
        ingestDocument.removeField(Feature.TYPE_KEY);
        feature.getProperties().forEach(ingestDocument::setFieldValue);
        if (ingestDocument.hasField(Feature.PROPERTIES_KEY)) { // properties are optional in Feature
            ingestDocument.removeField(Feature.PROPERTIES_KEY);
        }
        ingestDocument.setFieldValue(this.geoShapeField, feature.getGeometry());
        ingestDocument.removeField(Feature.GEOMETRY_KEY);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements org.opensearch.ingest.Processor.Factory {
        @Override
        public FeatureProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) {
            String geoShapeField = readStringProperty(TYPE, processorTag, config, FIELD_KEY);
            return new FeatureProcessor(processorTag, description, geoShapeField);
        }
    }
}
