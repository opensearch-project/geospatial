/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

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

import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.test.OpenSearchTestCase;

public class XYShapeSupportVisitorTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;

    private XYShapeSupportVisitor visitor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        visitor = new XYShapeSupportVisitor();
    }

    public void testPrepareForIndexingCircle() {
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> visitor.visit(randomCircle(randomBoolean()))
        );
        assertEquals("CIRCLE is not supported", exception.getMessage());
    }

    public void testPrepareForIndexingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES), randomBoolean());
        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> visitor.visit(ring));
        assertEquals("cannot index LINEARRING [ " + ring + " ] directly", exception.getMessage());
    }

    public void testPrepareForIndexingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit, randomBoolean());
        assertEquals(geometry, visitor.visit(geometry));
    }

    public void testPrepareForIndexingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit, randomBoolean());
        assertEquals(geometry, visitor.visit(geometry));
    }

    public void testPrepareForIndexingPoint() {
        Point geometry = randomPoint(randomBoolean());
        assertEquals(geometry, visitor.visit(geometry));
    }

    public void testPrepareForIndexingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit, randomBoolean());
        assertEquals(geometry, visitor.visit(geometry));
    }

    public void testPrepareForIndexingPolygon() throws IOException, ParseException {
        Polygon geometry = randomPolygon();
        assertEquals(geometry, visitor.visit(geometry));
    }

    public void testPrepareForIndexingMultiPolygon() throws IOException, ParseException {
        MultiPolygon geometry = randomMultiPolygon();
        assertEquals(geometry, visitor.visit(geometry));
    }

    public void testPrepareForIndexingGeometryCollection() {
        GeometryCollection<Geometry> collection = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS, randomBoolean());
        assertEquals(collection, visitor.visit(collection));
    }

    public void testPrepareForIndexingRectangle() {
        Rectangle geometry = randomRectangle();
        assertEquals(geometry, visitor.visit(geometry));
    }

}
