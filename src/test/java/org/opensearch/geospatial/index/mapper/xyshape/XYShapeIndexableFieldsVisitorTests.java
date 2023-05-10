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
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomMultiXYPolygon;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomPoint;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomRectangle;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomXYPolygon;

import java.io.IOException;
import java.text.ParseException;

import org.apache.lucene.index.IndexableField;
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
import org.opensearch.test.OpenSearchTestCase;

public class XYShapeIndexableFieldsVisitorTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;
    private GeometryVisitor<IndexableField[], RuntimeException> visitor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        visitor = new XYShapeIndexableFieldsVisitor(GeospatialTestHelper.randomLowerCaseString());
    }

    private int numberOfTrianglesFromLine(int vertices) {
        // Lucene index geometry as triangle. For ex: from 2 points, 1 triangle is made with third
        // vertex as first vertex
        if (vertices < 1) {
            throw new IllegalArgumentException("vertices count should be at least 1");
        }
        return vertices - 1;
    }

    private int numberOfTrianglesFromPoint() {
        // Lucene index geometry as triangle. Every point will be indexed as 1 triangle.
        return 1;
    }

    public void testIndexingCircle() {
        Circle circle = randomCircle(randomBoolean());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> visitor.visit(circle));
        assertEquals("invalid shape type found [ CIRCLE ] while indexing shape", exception.getMessage());
    }

    public void testIndexingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES), randomBoolean());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> visitor.visit(ring));
        assertEquals("invalid shape type found [ LINEARRING ] while indexing shape", exception.getMessage());
    }

    public void testIndexingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit, randomBoolean());
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        int expectedTrianglesSize = numberOfTrianglesFromLine(verticesLimit);
        assertEquals("indexing is incomplete", expectedTrianglesSize, indexableFields.length);
    }

    public void testIndexingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit, randomBoolean());
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        int expectedTrianglesSize = linesLimit * numberOfTrianglesFromLine(verticesLimit);
        assertEquals("indexing is incomplete", expectedTrianglesSize, indexableFields.length);
    }

    public void testIndexingPoint() {
        Point geometry = randomPoint(randomBoolean());
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        assertEquals("indexing is incomplete", numberOfTrianglesFromPoint(), indexableFields.length);
    }

    public void testIndexingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit, randomBoolean());
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        int expectedTrianglesSize = pointLimit * numberOfTrianglesFromPoint();
        assertEquals("indexing is incomplete", expectedTrianglesSize, indexableFields.length);
    }

    public void testIndexingPolygon() throws IOException, ParseException {
        Polygon geometry = randomXYPolygon();
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        assertTrue("indexable field list cannot be empty", indexableFields.length > 0);
    }

    public void testIndexingMultiPolygon() throws IOException, ParseException {
        MultiPolygon geometry = randomMultiXYPolygon();
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        assertTrue("indexable field list cannot be empty", indexableFields.length >= geometry.size());
    }

    public void testIndexingGeometryCollection() {
        GeometryCollection<Geometry> geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS, randomBoolean());
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        assertTrue("indexing is incomplete", indexableFields.length >= geometry.size());
    }

    public void testIndexingRectangle() {
        Rectangle geometry = randomRectangle();
        final IndexableField[] indexableFields = visitor.visit(geometry);
        assertNotNull("indexable field cannot be null", indexableFields);
        assertTrue("indexable field list cannot be empty", indexableFields.length > 0);
    }
}
