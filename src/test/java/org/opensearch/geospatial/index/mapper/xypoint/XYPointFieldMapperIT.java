/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Point;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder;

public class XYPointFieldMapperIT extends GeospatialRestTestCase {
    private static final String FIELD_X_KEY = "x";
    private static final String FIELD_Y_KEY = "y";

    public void testMappingWithXYPointField() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYPointFieldMapper.CONTENT_TYPE));
        final Map<String, Object> fieldNameTypeMap = getIndexProperties(indexName);
        assertTrue("field name is not found inside mapping", fieldNameTypeMap.containsKey(fieldName));
        final Map<String, Object> fieldType = (Map<String, Object>) fieldNameTypeMap.get(fieldName);
        assertEquals("invalid field type", XYPointFieldMapper.CONTENT_TYPE, fieldType.get(FIELD_TYPE_KEY));
        deleteIndex(indexName);
    }

    public void testIndexWithXYPointFieldAsWKTFormat() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYPointFieldMapper.CONTENT_TYPE));
        final Point point = ShapeObjectBuilder.randomPoint(randomBoolean());
        String docID = indexDocument(indexName, getDocumentWithWKTValueForXYPoint(fieldName, point));
        assertTrue("failed to index document", getIndexDocumentCount(indexName) > 0);
        final Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull("failed to get indexed document", document);
        assertEquals("failed to index xy_point", point.toString(), document.get(fieldName));
        deleteIndex(indexName);
    }

    public void testIndexWithXYPointFieldAsArrayFormat() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYPointFieldMapper.CONTENT_TYPE));
        final Point point = ShapeObjectBuilder.randomPoint(randomBoolean());
        String docID = indexDocument(indexName, getDocumentWithArrayValueForXYPoint(fieldName, point));
        assertTrue("failed to index document", getIndexDocumentCount(indexName) > 0);
        final Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull("failed to get indexed document", document);
        assertEquals("failed to index xy_point", List.of(point.getY(), point.getX()), document.get(fieldName));
        deleteIndex(indexName);
    }

    public void testIndexWithXYPointFieldAsStringFormat() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYPointFieldMapper.CONTENT_TYPE));
        final Point point = ShapeObjectBuilder.randomPoint(randomBoolean());
        String pointAsString = point.getX() + "," + point.getY();
        String docID = indexDocument(indexName, getDocumentWithStringValueForXYPoint(fieldName, pointAsString));
        assertTrue("failed to index document", getIndexDocumentCount(indexName) > 0);
        final Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull("failed to get indexed document", document);
        assertEquals("failed to index xy_point", pointAsString, document.get(fieldName));
        deleteIndex(indexName);
    }

    public void testIndexWithXYPointFieldAsObjectFormat() throws Exception {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String fieldName = GeospatialTestHelper.randomLowerCaseString();
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, XYPointFieldMapper.CONTENT_TYPE));
        final Point point = ShapeObjectBuilder.randomPoint(randomBoolean());
        String docID = indexDocument(indexName, getDocumentWithObjectValueForXYPoint(fieldName, point));
        assertTrue("failed to index document", getIndexDocumentCount(indexName) > 0);
        final Map<String, Object> document = getDocument(docID, indexName);
        assertNotNull("failed to get indexed document", document);
        String expectedValue = String.format(Locale.ROOT, "{x=%s, y=%s}", point.getX(), point.getY());
        assertEquals("failed to index xy_point", expectedValue, document.get(fieldName).toString());
        deleteIndex(indexName);
    }

    private String getDocumentWithWKTValueForXYPoint(String fieldName, Geometry geometry) throws Exception {
        return buildContentAsString(build -> build.field(fieldName, geometry.toString()));
    }

    private String getDocumentWithArrayValueForXYPoint(String fieldName, Point point) throws Exception {
        return buildContentAsString(build -> build.field(fieldName, new double[] { point.getY(), point.getX() }));
    }

    private String getDocumentWithStringValueForXYPoint(String fieldName, String pointAsString) throws Exception {
        return buildContentAsString(build -> build.field(fieldName, pointAsString));
    }

    private String getDocumentWithObjectValueForXYPoint(String fieldName, Point point) throws Exception {
        return buildContentAsString(build -> {
            build.startObject(fieldName);
            build.field(FIELD_X_KEY, point.getX());
            build.field(FIELD_Y_KEY, point.getY());
            build.endObject();
        });
    }

}
