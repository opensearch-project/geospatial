/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomCircle;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomGeometryCollection;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomLine;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomLinearRing;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomMultiLine;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomMultiPoint;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomMultiPolygon;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomPoint;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomPolygon;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomRectangle;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomShapeRelation;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.test.OpenSearchTestCase;

public class XYShapeQueryProcessorTests extends OpenSearchTestCase {

    public static final boolean VALID_FIELD_TYPE = true;
    public static final boolean INVALID_FIELD_TYPE = false;
    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;
    private GeometryVisitor<List<XYGeometry>, RuntimeException> mockQueryVisitor;
    private GeometryVisitor<Geometry, RuntimeException> mockSupportVisitor;
    private XYShapeQueryProcessor queryProcessor;
    private QueryShardContext mockQueryShardContext;
    private String fieldName;
    private ShapeRelation relation;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockQueryShardContext = mock(QueryShardContext.class);
        mockQueryVisitor = mock(GeometryVisitor.class);
        queryProcessor = new XYShapeQueryProcessor();
        fieldName = GeospatialTestHelper.randomLowerCaseString();
        relation = randomShapeRelation();
    }

    public void testQueryingNullGeometry() {
        mockFieldType(VALID_FIELD_TYPE);
        final Query query = queryProcessor.shapeQuery(null, fieldName, relation, mockQueryVisitor, mockQueryShardContext);
        assertEquals("No match found query should be returned", new MatchNoDocsQuery(), query);
    }

    public void testQueryingNullFieldName() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), null, relation, mockQueryVisitor, mockQueryShardContext)
        );
        assertEquals("Unexpected error message", "Field name cannot be null", exception.getMessage());
    }

    public void testQueryingNullQueryContext() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, relation, mockQueryVisitor, null)
        );
        assertEquals("Unexpected error message", "QueryShardContext cannot be null", exception.getMessage());
    }

    public void testQueryingNullShapeRelation() {
        mockFieldType(VALID_FIELD_TYPE);
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, null, mockQueryVisitor, mockQueryShardContext)
        );
        assertEquals("Unexpected error message", "ShapeRelation cannot be null", exception.getMessage());
    }

    public void testQueryingEmptyGeometry() {
        mockFieldType(VALID_FIELD_TYPE);
        final Query query = queryProcessor.shapeQuery(
            new GeometryCollection<>(),
            fieldName,
            relation,
            mockQueryVisitor,
            mockQueryShardContext
        );
        assertEquals("No match found query should be returned", new MatchNoDocsQuery(), query);
    }

    public void testQueryingInvalidFieldTypeGeometry() {
        mockFieldType(INVALID_FIELD_TYPE);
        final QueryShardException exception = expectThrows(
            QueryShardException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        assertEquals(
            "wrong exception message",
            "Expected xy_shape field type for Field [ " + fieldName + " ] but found geo_point",
            exception.getMessage()
        );
    }

    public void testQueryingCircle() {
        mockFieldType(VALID_FIELD_TYPE);
        Circle circle = randomCircle();
        when(mockQueryVisitor.visit(circle)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(circle, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(circle);
    }

    public void testQueryingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES));
        expectThrows(
            NullPointerException.class,
            () -> queryProcessor.shapeQuery(ring, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
    }

    public void testQueryingLine() {
        mockFieldType(VALID_FIELD_TYPE);
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit);
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingMultiLine() {
        mockFieldType(VALID_FIELD_TYPE);
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit);
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingPoint() {
        mockFieldType(VALID_FIELD_TYPE);
        Point geometry = randomPoint();
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingMultiPoint() {
        mockFieldType(VALID_FIELD_TYPE);
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit);
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingPolygon() throws IOException, ParseException {
        mockFieldType(VALID_FIELD_TYPE);
        Polygon geometry = randomPolygon();
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingMultiPolygon() throws IOException, ParseException {
        mockFieldType(VALID_FIELD_TYPE);
        MultiPolygon geometry = randomMultiPolygon();
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingGeometryCollection() throws IOException, ParseException {
        mockFieldType(VALID_FIELD_TYPE);
        GeometryCollection geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    public void testQueryingEmptyGeometryCollection() throws IOException, ParseException {
        mockFieldType(VALID_FIELD_TYPE);
        GeometryCollection geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of());
        final Query actualQuery = queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext);
        assertNotNull("failed to convert to Query", actualQuery);
        verify(mockQueryVisitor).visit(geometry);
        assertEquals("MatchNoDocs query should be returned", new MatchNoDocsQuery(), actualQuery);
    }

    public void testQueryingRectangle() {
        mockFieldType(VALID_FIELD_TYPE);
        Rectangle geometry = randomRectangle();
        when(mockQueryVisitor.visit(geometry)).thenReturn(List.of(mock(XYGeometry.class)));
        assertNotNull(
            "failed to convert to Query",
            queryProcessor.shapeQuery(geometry, fieldName, relation, mockQueryVisitor, mockQueryShardContext)
        );
        verify(mockQueryVisitor).visit(geometry);
    }

    private void mockFieldType(boolean success) {
        if (success) {
            when(mockQueryShardContext.fieldMapper(fieldName)).thenReturn(
                new XYShapeFieldMapper.XYShapeFieldType(fieldName, randomBoolean(), randomBoolean(), randomBoolean(), emptyMap())
            );
            return;
        }
        when(mockQueryShardContext.fieldMapper(fieldName)).thenReturn(new GeoPointFieldMapper.GeoPointFieldType(fieldName));
    }

}
