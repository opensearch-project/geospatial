/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.FIELD_DATA;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.GEOSPATIAL_DEFAULT_FIELD_NAME;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.MAX_SUPPORTED_GEOJSON_FEATURE_COUNT;

import java.util.Collections;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.settings.GeospatialSettings;
import org.opensearch.geospatial.settings.GeospatialSettingsAccessor;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestContentTests extends OpenSearchTestCase {
    private static int MIN_FEATURE_COUNT = 3;
    private String indexName;
    private String fieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        GeospatialTestHelper.initializeGeoJSONRequestContentSettings();
        indexName = randomLowerCaseString();
        fieldName = randomLowerCaseString();
    }

    private Map<String, Object> buildRequestContent(String indexName, String fieldName, int count) {
        final var contents = new JSONObject();
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), indexName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), fieldName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName(), "geo_shape");
        JSONArray values = new JSONArray();
        for (int i = 0; i < count; i++) {
            values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        }
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents.toMap();
    }

    public void testCreate() {
        Map<String, Object> contents = buildRequestContent(indexName, fieldName, MIN_FEATURE_COUNT);
        final var content = UploadGeoJSONRequestContent.create(contents);
        assertNotNull(content);
        assertEquals(fieldName, content.getFieldName());
        assertEquals(indexName, content.getIndexName());
        assertEquals(contents.get(FIELD_DATA.getPreferredName()), content.getData());
    }

    public void testCreateEmptyIndexName() {
        IllegalArgumentException invalidIndexName = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(buildRequestContent("", "location", MIN_FEATURE_COUNT))
        );
        assertTrue(invalidIndexName.getMessage().contains("[ index ] cannot be empty"));
    }

    public void testCreateWithOneMoreThanMaxSupportedFeatureCount() {
        int featureCount = MAX_SUPPORTED_GEOJSON_FEATURE_COUNT + 1;
        IllegalArgumentException reachedMaxFeatureCount = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(buildRequestContent(indexName, fieldName, featureCount))
        );
        assertEquals(
            "wrong error returned",
            reachedMaxFeatureCount.getMessage(),
            "Received 10001 features, but, cannot upload more than 10000 features"
        );
    }

    public void testCreateEmptyGeospatialFieldName() {
        final var content = UploadGeoJSONRequestContent.create(buildRequestContent(randomLowerCaseString(), "", MIN_FEATURE_COUNT));
        assertNotNull(content);
        assertEquals("wrong field name", GEOSPATIAL_DEFAULT_FIELD_NAME, content.getFieldName());
    }

    public void testCreateEmptyGeospatialFieldType() {
        Map<String, Object> contents = buildRequestContent(indexName, fieldName, MIN_FEATURE_COUNT);
        contents.remove(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName());
        IllegalArgumentException invalidIndexName = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(contents)
        );
        assertTrue(invalidIndexName.getMessage().contains("[ type ] cannot be empty"));
    }

    public void testValidLineString() {
        // Simple LineString with 100 coordinates - should work fine
        JSONArray coordinates = new JSONArray();
        for (int i = 0; i < 100; i++) {
            coordinates.put(new JSONArray().put(i * 0.1).put(45.0));
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "LineString").put("coordinates", coordinates))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testLineStringWithTooManyCoordinates() {
        // LineString with 10,001 coordinates - should fail
        JSONArray coordinates = new JSONArray();
        for (int i = 0; i <= 10000; i++) {
            coordinates.put(new JSONArray().put(i * 0.01).put(45.0));
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "LineString").put("coordinates", coordinates))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testValidPolygonWithHoles() {
        // Simple polygon with 10 holes - should work fine
        JSONArray outerRing = new JSONArray().put(new JSONArray().put(-10.0).put(-10.0))
            .put(new JSONArray().put(10.0).put(-10.0))
            .put(new JSONArray().put(10.0).put(10.0))
            .put(new JSONArray().put(-10.0).put(10.0))
            .put(new JSONArray().put(-10.0).put(-10.0));

        JSONArray rings = new JSONArray().put(outerRing);

        // Add 10 holes
        for (int i = 0; i < 10; i++) {
            JSONArray hole = new JSONArray().put(new JSONArray().put(i * 0.5).put(0.0))
                .put(new JSONArray().put(i * 0.5 + 0.2).put(0.0))
                .put(new JSONArray().put(i * 0.5 + 0.2).put(0.2))
                .put(new JSONArray().put(i * 0.5).put(0.2))
                .put(new JSONArray().put(i * 0.5).put(0.0));
            rings.put(hole);
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "Polygon").put("coordinates", rings))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testPolygonWithTooManyHoles() {
        // Polygon with 1,001 holes - should fail
        JSONArray outerRing = new JSONArray().put(new JSONArray().put(-50.0).put(-50.0))
            .put(new JSONArray().put(50.0).put(-50.0))
            .put(new JSONArray().put(50.0).put(50.0))
            .put(new JSONArray().put(-50.0).put(50.0))
            .put(new JSONArray().put(-50.0).put(-50.0));

        JSONArray rings = new JSONArray().put(outerRing);

        // Add 1,001 holes
        for (int i = 0; i <= 1000; i++) {
            JSONArray hole = new JSONArray().put(new JSONArray().put(0.0).put(i * 0.01))
                .put(new JSONArray().put(0.1).put(i * 0.01))
                .put(new JSONArray().put(0.1).put(i * 0.01 + 0.1))
                .put(new JSONArray().put(0.0).put(i * 0.01 + 0.1))
                .put(new JSONArray().put(0.0).put(i * 0.01));
            rings.put(hole);
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "Polygon").put("coordinates", rings))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testPolygonHoleWithTooManyCoordinates() {
        // Polygon with hole that has 10,001 coordinates - should fail
        JSONArray outerRing = new JSONArray().put(new JSONArray().put(-50.0).put(-50.0))
            .put(new JSONArray().put(50.0).put(-50.0))
            .put(new JSONArray().put(50.0).put(50.0))
            .put(new JSONArray().put(-50.0).put(50.0))
            .put(new JSONArray().put(-50.0).put(-50.0));

        // Create hole with 10,001 coordinates
        JSONArray hole = new JSONArray();
        for (int i = 0; i <= 10000; i++) {
            hole.put(new JSONArray().put(i * 0.001).put(0.0));
        }

        JSONArray rings = new JSONArray().put(outerRing).put(hole);

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "Polygon").put("coordinates", rings))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testValidMultiLineString() {
        // MultiLineString with 10 LineStrings - should work fine
        JSONArray lineStrings = new JSONArray();
        for (int i = 0; i < 10; i++) {
            JSONArray line = new JSONArray().put(new JSONArray().put(i * 1.0).put(0.0)).put(new JSONArray().put(i * 1.0 + 0.5).put(0.5));
            lineStrings.put(line);
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "MultiLineString").put("coordinates", lineStrings))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testMultiLineStringWithTooManyLineStrings() {
        // MultiLineString with 101 LineStrings - should fail
        JSONArray lineStrings = new JSONArray();
        for (int i = 0; i <= 100; i++) {
            JSONArray line = new JSONArray().put(new JSONArray().put(i * 0.1).put(0.0)).put(new JSONArray().put(i * 0.1 + 0.05).put(0.05));
            lineStrings.put(line);
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "MultiLineString").put("coordinates", lineStrings))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testValidMultiPolygon() {
        // MultiPolygon with 5 simple polygons - should work fine
        JSONArray polygons = new JSONArray();
        for (int i = 0; i < 5; i++) {
            JSONArray ring = new JSONArray().put(new JSONArray().put(i * 2.0).put(0.0))
                .put(new JSONArray().put(i * 2.0 + 1.0).put(0.0))
                .put(new JSONArray().put(i * 2.0 + 1.0).put(1.0))
                .put(new JSONArray().put(i * 2.0).put(1.0))
                .put(new JSONArray().put(i * 2.0).put(0.0));
            polygons.put(new JSONArray().put(ring));
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "MultiPolygon").put("coordinates", polygons))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testMultiPolygonWithTooManyPolygons() {
        // MultiPolygon with 101 polygons - should fail
        JSONArray polygons = new JSONArray();
        for (int i = 0; i <= 100; i++) {
            JSONArray ring = new JSONArray().put(new JSONArray().put(i * 0.1).put(0.0))
                .put(new JSONArray().put(i * 0.1 + 0.05).put(0.0))
                .put(new JSONArray().put(i * 0.1 + 0.05).put(0.05))
                .put(new JSONArray().put(i * 0.1).put(0.05))
                .put(new JSONArray().put(i * 0.1).put(0.0));
            polygons.put(new JSONArray().put(ring));
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "MultiPolygon").put("coordinates", polygons))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testValidGeometryCollection() {
        // GeometryCollection with a few geometries - should work fine
        JSONArray geometries = new JSONArray();

        // Add a Point
        geometries.put(new JSONObject().put("type", "Point").put("coordinates", new JSONArray().put(0.0).put(0.0)));

        // Add a LineString
        geometries.put(
            new JSONObject().put("type", "LineString")
                .put("coordinates", new JSONArray().put(new JSONArray().put(0.0).put(0.0)).put(new JSONArray().put(1.0).put(1.0)))
        );

        // Add a Polygon
        JSONArray ring = new JSONArray().put(new JSONArray().put(0.0).put(0.0))
            .put(new JSONArray().put(1.0).put(0.0))
            .put(new JSONArray().put(1.0).put(1.0))
            .put(new JSONArray().put(0.0).put(1.0))
            .put(new JSONArray().put(0.0).put(0.0));
        geometries.put(new JSONObject().put("type", "Polygon").put("coordinates", new JSONArray().put(ring)));

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "GeometryCollection").put("geometries", geometries))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testGeometryCollectionWithTooManyGeometries() {
        // GeometryCollection with 101 geometries - should fail
        JSONArray geometries = new JSONArray();
        for (int i = 0; i <= 100; i++) {
            geometries.put(new JSONObject().put("type", "Point").put("coordinates", new JSONArray().put(i * 0.1).put(0.0)));
        }

        JSONObject feature = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "GeometryCollection").put("geometries", geometries))
            .put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testGeometryCollectionWithTooDeepNesting() {
        // Create deeply nested GeometryCollections (depth 6) - should fail
        JSONObject innermost = new JSONObject().put("type", "Point").put("coordinates", new JSONArray().put(0.0).put(0.0));

        JSONObject geometry = innermost;
        for (int i = 0; i < 6; i++) {
            geometry = new JSONObject().put("type", "GeometryCollection").put("geometries", new JSONArray().put(geometry));
        }

        JSONObject feature = new JSONObject().put("type", "Feature").put("geometry", geometry).put("properties", new JSONObject());

        JSONObject request = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
    }

    public void testDynamicSettingsValidation() {
        // Test that validation respects dynamically configured settings
        // Lower the MAX_COORDINATES_PER_GEOMETRY limit from 10,000 to 100

        Settings customSettings = Settings.builder()
            .put(GeospatialSettings.MAX_COORDINATES_PER_GEOMETRY.getKey(), 100)
            .put(GeospatialSettings.MAX_HOLES_PER_POLYGON.getKey(), 1_000)
            .put(GeospatialSettings.MAX_MULTI_GEOMETRIES.getKey(), 100)
            .put(GeospatialSettings.MAX_GEOMETRY_COLLECTION_NESTED_DEPTH.getKey(), 5)
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(
            customSettings,
            java.util.Set.of(
                GeospatialSettings.MAX_COORDINATES_PER_GEOMETRY,
                GeospatialSettings.MAX_HOLES_PER_POLYGON,
                GeospatialSettings.MAX_MULTI_GEOMETRIES,
                GeospatialSettings.MAX_GEOMETRY_COLLECTION_NESTED_DEPTH
            )
        );

        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getClusterSettings()).thenReturn(clusterSettings);

        // Re-initialize with custom lower limit
        GeospatialSettingsAccessor customAccessor = new GeospatialSettingsAccessor(mockClusterService, customSettings);
        UploadGeoJSONRequestContent.initialize(customAccessor);

        // Test 1: LineString with 101 coordinates should FAIL (exceeds new limit of 100)
        JSONArray coordinates101 = new JSONArray();
        for (int i = 0; i <= 100; i++) {
            coordinates101.put(new JSONArray().put(i * 0.01).put(45.0));
        }

        JSONObject feature101 = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "LineString").put("coordinates", coordinates101))
            .put("properties", new JSONObject());

        JSONObject request101 = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature101));

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request101.toMap())
        );

        // Test 2: LineString with 99 coordinates should PASS (within new limit of 100)
        JSONArray coordinates99 = new JSONArray();
        for (int i = 0; i < 99; i++) {
            coordinates99.put(new JSONArray().put(i * 0.01).put(45.0));
        }

        JSONObject feature99 = new JSONObject().put("type", "Feature")
            .put("geometry", new JSONObject().put("type", "LineString").put("coordinates", coordinates99))
            .put("properties", new JSONObject());

        JSONObject request99 = new JSONObject().put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature99));

        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request99.toMap());
        assertNotNull("Should successfully create content with 99 coordinates", content);

        // Restore default settings for other tests
        GeospatialTestHelper.initializeGeoJSONRequestContentSettings();
    }
}
