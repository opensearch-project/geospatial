/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

import java.io.IOException;
import java.util.Map;

import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Point;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder;

public class XYShapeFieldMapperIT extends GeospatialRestTestCase {

    private static final String FIELD_COORDINATES_KEY = "coordinates";

    private String getDocumentWithGeoJSONValueForXYShape(String fieldName, Point point) throws IOException {
        return buildContentAsString(build -> {
            build.startObject(fieldName);
            build.field(FIELD_TYPE_KEY, GeoJson.getGeoJsonName(point));
            build.field(FIELD_COORDINATES_KEY, new double[] { point.getX(), point.getY() });
            build.endObject();
        });
    }

    private String getDocumentWithWKTValueForXYShape(String fieldName, Geometry geometry) throws IOException {
        return buildContentAsString(build -> build.field(fieldName, geometry.toString()));
    }

    public void testMappingWithXYShapeField() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYShapeFieldMapper.CONTENT_TYPE));
        final Map<String, Object> fieldNameTypeMap = getIndexProperties(indexName);
        assertTrue("field name is not found inside mapping", fieldNameTypeMap.containsKey(fieldName));
        final Map<String, Object> fieldType = (Map<String, Object>) fieldNameTypeMap.get(fieldName);
        assertEquals("invalid field type", XYShapeFieldMapper.CONTENT_TYPE, fieldType.get(FIELD_TYPE_KEY));
        deleteIndex(indexName);
    }

    public void testIndexWithXYShapeFieldAsWKTFormat() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYShapeFieldMapper.CONTENT_TYPE));
        final Point point = ShapeObjectBuilder.randomPoint(randomBoolean());
        String docID = indexDocument(indexName, getDocumentWithWKTValueForXYShape(fieldName, point));
        assertTrue("failed to index document", getIndexDocumentCount(indexName) > 0);
        final Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull("failed to get indexed document", document);
        assertEquals("failed to index xy_shape", point.toString(), document.get(fieldName));
        deleteIndex(indexName);
    }

    public void testIndexWithXYShapeFieldAsGeoJSONFormat() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYShapeFieldMapper.CONTENT_TYPE));
        final Point point = ShapeObjectBuilder.randomPoint(randomBoolean());
        String docID = indexDocument(indexName, getDocumentWithGeoJSONValueForXYShape(fieldName, point));
        assertTrue("failed to index document", getIndexDocumentCount(indexName) > 0);
        final Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull("failed to get indexed document", document);
        // remove z value since Shape will always ignore z value
        final Map<String, Object> geoJSON = GeoJson.toMap(new Point(point.getX(), point.getY()));
        assertEquals("failed to index xy_shape", geoJSON, document.get(fieldName));
        deleteIndex(indexName);
    }
}
