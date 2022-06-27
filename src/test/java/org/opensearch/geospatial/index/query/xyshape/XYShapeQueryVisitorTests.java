/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import static org.mockito.Mockito.mock;
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

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.opensearch.geometry.Circle;
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
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.test.OpenSearchTestCase;

public class XYShapeQueryVisitorTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;
    public static final int FIRST_GEOMETRY = 0;
    public static final int SIZE = 1;
    private GeometryVisitor<List<XYGeometry>, RuntimeException> queryVisitor;
    private String fieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        QueryShardContext context = mock(QueryShardContext.class);
        fieldName = GeospatialTestHelper.randomLowerCaseString();
        queryVisitor = new XYShapeQueryVisitor(fieldName, context);
    }

    public void testQueryingCircle() {
        Circle circle = randomCircle(randomBoolean());
        final List<XYGeometry> geometries = queryVisitor.visit(circle);
        assertNotNull("failed to convert to XYCircle", geometries);
        assertEquals("Unexpected number of geomteries found", SIZE, geometries.size());
        assertTrue("invalid object found", geometries.get(FIRST_GEOMETRY) instanceof XYCircle);
    }

    public void testQueryingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES), randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> queryVisitor.visit(ring));
        assertEquals("Field [" + fieldName + "] found an unsupported shape LinearRing", exception.getMessage());
    }

    public void testQueryingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit, randomBoolean());
        final List<XYGeometry> geometries = queryVisitor.visit(geometry);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", SIZE, geometries.size());
        assertTrue("invalid object found ", geometries.get(FIRST_GEOMETRY) instanceof XYLine);
    }

    public void testQueryingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine multiLine = randomMultiLine(verticesLimit, linesLimit, randomBoolean());
        final List<XYGeometry> geometries = queryVisitor.visit(multiLine);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", geometries.size(), multiLine.size());
        for (XYGeometry geometry : geometries) {
            assertTrue("invalid object found", geometry instanceof XYLine);
        }
    }

    public void testQueryingPoint() {
        Point geometry = randomPoint(randomBoolean());
        final List<XYGeometry> geometries = queryVisitor.visit(geometry);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", SIZE, geometries.size());
        assertTrue("invalid object found", geometries.get(FIRST_GEOMETRY) instanceof XYPoint);

    }

    public void testQueryingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint multiPoint = randomMultiPoint(pointLimit, randomBoolean());
        final List<XYGeometry> geometries = queryVisitor.visit(multiPoint);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", geometries.size(), multiPoint.size());
        for (XYGeometry geometry : geometries) {
            assertTrue("invalid object found", geometry instanceof XYPoint);
        }
    }

    public void testQueryingPolygon() throws IOException, ParseException {
        Polygon geometry = randomPolygon();
        final List<XYGeometry> geometries = queryVisitor.visit(geometry);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", SIZE, geometries.size());
        assertTrue("invalid object found", geometries.get(FIRST_GEOMETRY) instanceof XYPolygon);
    }

    public void testQueryingMultiPolygon() throws IOException, ParseException {
        MultiPolygon multiPolygon = randomMultiPolygon();
        final List<XYGeometry> geometries = queryVisitor.visit(multiPolygon);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", geometries.size(), multiPolygon.size());
        for (XYGeometry geometry : geometries) {
            assertTrue("invalid object found", geometry instanceof XYPolygon);
        }
    }

    public void testQueryingGeometryCollection() {
        GeometryCollection<?> collection = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS, randomBoolean());
        final List<XYGeometry> geometries = queryVisitor.visit(collection);
        assertNotNull("Query geometries cannot be null", geometries);
        assertTrue("Some geometries are not processed", geometries.size() >= collection.size());
    }

    public void testQueryingRectangle() {
        Rectangle geometry = randomRectangle();
        final List<XYGeometry> geometries = queryVisitor.visit(geometry);
        assertNotNull("Query geometries cannot be null", geometries);
        assertEquals("Unexpected number of geomteries found", SIZE, geometries.size());
        assertTrue("invalid object found", geometries.get(FIRST_GEOMETRY) instanceof XYRectangle);
    }
}
