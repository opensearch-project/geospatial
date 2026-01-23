/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialParser.extractValueAsString;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.geospatial.GeospatialParser;
import org.opensearch.geospatial.geojson.Feature;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * UploadGeoJSONRequestContent is the Data model for UploadGeoJSONRequest's body
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class UploadGeoJSONRequestContent {

    public static final String GEOSPATIAL_DEFAULT_FIELD_NAME = "location";
    public static final ParseField FIELD_INDEX = new ParseField("index");
    public static final ParseField FIELD_GEOSPATIAL = new ParseField("field");
    public static final ParseField FIELD_GEOSPATIAL_TYPE = new ParseField("type");
    public static final ParseField FIELD_DATA = new ParseField("data");

    // Custom Vector Map can support fetching up to 10K Features. Hence, we chose same value as limit
    // for upload as well.
    public static final int MAX_SUPPORTED_GEOJSON_FEATURE_COUNT = 10_000;

    // Geometric complexity limits for resource management
    private static final int MAX_COORDINATES_PER_GEOMETRY = 10_000;
    private static final int MAX_HOLES_PER_POLYGON = 1_000;
    private static final int MAX_MULTI_GEOMETRIES = 100;
    private static final int MAX_GEOMETRY_COLLECTION_DEPTH = 5;

    // GeoJSON geometry type constants
    private static final String GEOMETRY_TYPE_LINESTRING = "LineString";
    private static final String GEOMETRY_TYPE_POLYGON = "Polygon";
    private static final String GEOMETRY_TYPE_MULTILINESTRING = "MultiLineString";
    private static final String GEOMETRY_TYPE_MULTIPOLYGON = "MultiPolygon";
    private static final String GEOMETRY_TYPE_GEOMETRYCOLLECTION = "GeometryCollection";
    private final String indexName;
    private final String fieldName;
    private final String fieldType;
    private final List<Object> data;

    /**
     * Creates UploadGeoJSONRequestContent from the user input
     * @param input user input of type Map
     * @return UploadGeoJSONRequestContent based on value from input
     * @throws NullPointerException if input is null
     * @throws IllegalArgumentException if input doesn't have valid arguments
     */
    public static UploadGeoJSONRequestContent create(Map<String, Object> input) {
        Objects.requireNonNull(input, "input cannot be null");
        final String index = validateIndexName(input);
        String fieldName = extractValueAsString(input, FIELD_GEOSPATIAL.getPreferredName());
        if (!Strings.hasText(fieldName)) {
            fieldName = GEOSPATIAL_DEFAULT_FIELD_NAME; // use default filed name, if field name is empty
        }
        final String fieldType = extractValueAsString(input, FIELD_GEOSPATIAL_TYPE.getPreferredName());
        if (!Strings.hasText(fieldType)) {
            throw new IllegalArgumentException("field [ " + FIELD_GEOSPATIAL_TYPE.getPreferredName() + " ] cannot be empty");
        }
        final Object geoJSONData = Objects.requireNonNull(
            input.get(FIELD_DATA.getPreferredName()),
            "field [ " + FIELD_DATA.getPreferredName() + " ] cannot be empty"
        );
        if (!(geoJSONData instanceof List)) {
            throw new IllegalArgumentException(
                geoJSONData + " is not an instance of List, but of type [ " + geoJSONData.getClass().getName() + " ]"
            );
        }
        validateFeatureCount(geoJSONData);
        // validateGeometricComplexity(geoJSONData);
        return new UploadGeoJSONRequestContent(index, fieldName, fieldType, (List<Object>) geoJSONData);
    }

    private static void validateFeatureCount(Object geoJSONData) {
        final long featureCount = getFeatureCount(geoJSONData);
        if (featureCount > MAX_SUPPORTED_GEOJSON_FEATURE_COUNT) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Received %d features, but, cannot upload more than %d features",
                    featureCount,
                    MAX_SUPPORTED_GEOJSON_FEATURE_COUNT
                )
            );
        }
    }

    private static long getFeatureCount(Object geoJSONData) {
        return ((List<Object>) geoJSONData).stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(GeospatialParser::getFeatures)
            .flatMap(List::stream)
            .count();
    }

    /**
     * Validates geometric complexity limits
     * @param geoJSONData input data containing features
     */
    private static void validateGeometricComplexity(Object geoJSONData) {
        List<Object> dataList = (List<Object>) geoJSONData;

        for (Object item : dataList) {
            Map<String, Object> geoJSON = GeospatialParser.toStringObjectMap(item);
            List<Map<String, Object>> features = GeospatialParser.getFeatures(geoJSON);

            for (Map<String, Object> feature : features) {
                Object geometryObj = feature.get(Feature.GEOMETRY_KEY);
                if (geometryObj == null) continue;

                Map<String, Object> geometry = GeospatialParser.toStringObjectMap(geometryObj);
                validateGeometry(geometry, 0);
            }
        }
    }

    /**
     * Recursively validates a geometry object
     * @param geometry the geometry to validate
     * @param depth current nesting depth for GeometryCollection
     */
    private static void validateGeometry(Map<String, Object> geometry, int depth) {
        if (depth > MAX_GEOMETRY_COLLECTION_DEPTH) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "GeometryCollection nesting depth %d exceeds limit of %d", depth, MAX_GEOMETRY_COLLECTION_DEPTH)
            );
        }

        String type = GeospatialParser.extractValueAsString(geometry, "type");
        if (type == null) return;

        // Handle GeometryCollection separately (uses "geometries" not "coordinates")
        if (GEOMETRY_TYPE_GEOMETRYCOLLECTION.equals(type)) {
            Object geometries = geometry.get("geometries");
            if (!(geometries instanceof List)) return;
            validateGeometryCollection((List<?>) geometries, depth);
            return;
        }

        // All other geometry types use "coordinates" - validate early
        Object coordinates = geometry.get("coordinates");
        if (!(coordinates instanceof List)) return;

        List<?> coordList = (List<?>) coordinates;

        // Validate based on geometry type
        switch (type) {
            case GEOMETRY_TYPE_LINESTRING:
                validateLineString(coordList);
                break;
            case GEOMETRY_TYPE_POLYGON:
                validatePolygon(coordList);
                break;
            case GEOMETRY_TYPE_MULTILINESTRING:
                validateMultiLineString(coordList);
                break;
            case GEOMETRY_TYPE_MULTIPOLYGON:
                validateMultiPolygon(coordList);
                break;
            default:
                // Unknown geometry type, skip validation
                break;
        }
    }

    /**
     * Validates LineString coordinate count
     */
    private static void validateLineString(List<?> coordinates) {
        if (coordinates.size() > MAX_COORDINATES_PER_GEOMETRY) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "LineString has %d coordinates, exceeds limit of %d",
                    coordinates.size(),
                    MAX_COORDINATES_PER_GEOMETRY
                )
            );
        }
    }

    /**
     * Validates Polygon rings (outer ring and holes)
     */
    private static void validatePolygon(List<?> rings) {
        int holes = rings.size() - 1;
        if (holes > MAX_HOLES_PER_POLYGON) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Polygon has %d holes, exceeds limit of %d", holes, MAX_HOLES_PER_POLYGON)
            );
        }

        // Validate outer ring
        if (!rings.isEmpty() && rings.get(0) instanceof List) {
            List<?> outerRing = (List<?>) rings.get(0);
            if (outerRing.size() > MAX_COORDINATES_PER_GEOMETRY) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Polygon outer ring has %d coordinates, exceeds limit of %d",
                        outerRing.size(),
                        MAX_COORDINATES_PER_GEOMETRY
                    )
                );
            }
        }

        // Validate each hole (inner ring)
        for (int i = 1; i < rings.size(); i++) {
            if (rings.get(i) instanceof List) {
                List<?> hole = (List<?>) rings.get(i);
                if (hole.size() > MAX_COORDINATES_PER_GEOMETRY) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Polygon hole %d has %d coordinates, exceeds limit of %d",
                            i,
                            hole.size(),
                            MAX_COORDINATES_PER_GEOMETRY
                        )
                    );
                }
            }
        }
    }

    /**
     * Validates MultiLineString collection
     */
    private static void validateMultiLineString(List<?> lineStrings) {
        if (lineStrings.size() > MAX_MULTI_GEOMETRIES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "MultiLineString has %d LineStrings, exceeds limit of %d",
                    lineStrings.size(),
                    MAX_MULTI_GEOMETRIES
                )
            );
        }

        for (Object lineObj : lineStrings) {
            if (lineObj instanceof List) {
                validateLineString((List<?>) lineObj);
            }
        }
    }

    /**
     * Validates MultiPolygon collection
     */
    private static void validateMultiPolygon(List<?> polygons) {
        if (polygons.size() > MAX_MULTI_GEOMETRIES) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "MultiPolygon has %d Polygons, exceeds limit of %d", polygons.size(), MAX_MULTI_GEOMETRIES)
            );
        }

        for (Object polyObj : polygons) {
            if (polyObj instanceof List) {
                validatePolygon((List<?>) polyObj);
            }
        }
    }

    /**
     * Validates GeometryCollection recursively
     */
    private static void validateGeometryCollection(List<?> geometries, int depth) {
        if (geometries.size() > MAX_MULTI_GEOMETRIES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "GeometryCollection has %d geometries, exceeds limit of %d",
                    geometries.size(),
                    MAX_MULTI_GEOMETRIES
                )
            );
        }

        for (Object geomObj : geometries) {
            if (geomObj instanceof Map) {
                Map<String, Object> geom = (Map<String, Object>) geomObj;
                validateGeometry(geom, depth + 1);
            }
        }
    }

    private static String validateIndexName(Map<String, Object> input) {
        String index = extractValueAsString(input, FIELD_INDEX.getPreferredName());
        if (Strings.hasText(index)) {
            return index;
        }
        throw new IllegalArgumentException(
            String.format(Locale.getDefault(), "field [ %s ] cannot be empty", FIELD_INDEX.getPreferredName())
        );
    }

    public String getIndexName() {
        return indexName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<Object> getData() {
        return data;
    }

    public String getFieldType() {
        return fieldType;
    }
}
