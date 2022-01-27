/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.Map;

import org.json.JSONObject;
import org.opensearch.geospatial.geojson.Feature;

public class GeospatialObjectBuilder {

    public static final String GEOMETRY_TYPE_KEY = "type";
    public static final String GEOMETRY_COORDINATES_KEY = "coordinates";

    public static JSONObject buildGeometry(String type, Object value) {
        JSONObject geometry = new JSONObject();
        geometry.put(GEOMETRY_TYPE_KEY, type);
        geometry.put(GEOMETRY_COORDINATES_KEY, value);
        return geometry;
    }

    public static JSONObject buildGeoJSONFeature(JSONObject geometry, JSONObject properties) {
        JSONObject feature = new JSONObject();
        feature.put(Feature.TYPE_KEY, Feature.TYPE);
        feature.put(Feature.GEOMETRY_KEY, geometry);
        feature.put(Feature.PROPERTIES_KEY, properties);
        return feature;
    }

    public static JSONObject buildProperties(Map<String, Object> properties) {
        JSONObject propertiesObject = new JSONObject();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            propertiesObject.put(entry.getKey(), entry.getValue());
        }
        return propertiesObject;
    }
}
