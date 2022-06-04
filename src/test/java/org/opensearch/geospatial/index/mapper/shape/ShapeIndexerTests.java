/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.shape;

import static org.mockito.Mockito.mock;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomCircle;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomGeometryCollection;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomLine;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomLinearRing;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomMultiLine;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomMultiPoint;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomMultiPolygon;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomPoint;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomPolygon;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomRectangle;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.lucene.index.IndexableField;
import org.opensearch.geometry.Circle;
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
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.test.OpenSearchTestCase;

public class ShapeIndexerTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;
    private ShapeIndexer indexer;
    private ParseContext parseContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexer = new ShapeIndexer(GeospatialTestHelper.randomLowerCaseString());
        parseContext = mock(ParseContext.class);
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
        Circle circle = randomCircle();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> indexer.indexShape(parseContext, circle));
        assertEquals("invalid shape type found [ CIRCLE ] while indexing shape", exception.getMessage());
    }

    public void testIndexingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> indexer.indexShape(parseContext, ring));
        assertEquals("invalid shape type found [ LINEARRING ] while indexing shape", exception.getMessage());
    }

    public void testIndexingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit);
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        int expectedTrianglesSize = numberOfTrianglesFromLine(verticesLimit);
        assertEquals(expectedTrianglesSize, indexableFields.size());
    }

    public void testIndexingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit);
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        int expectedTrianglesSize = linesLimit * numberOfTrianglesFromLine(verticesLimit);
        assertEquals(expectedTrianglesSize, indexableFields.size());
    }

    public void testIndexingPoint() {
        Point geometry = randomPoint();
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        assertEquals(numberOfTrianglesFromPoint(), indexableFields.size());
    }

    public void testIndexingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit);
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        int expectedTrianglesSize = pointLimit * numberOfTrianglesFromPoint();
        assertEquals(expectedTrianglesSize, indexableFields.size());
    }

    public void testIndexingPolygon() throws IOException, ParseException {
        Polygon geometry = randomPolygon();
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        assertFalse(indexableFields.isEmpty());
    }

    public void testIndexingMultiPolygon() throws IOException, ParseException {
        MultiPolygon geometry = randomMultiPolygon();
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        assertFalse(indexableFields.isEmpty());
    }

    public void testIndexingGeometryCollection() throws IOException, ParseException {
        GeometryCollection geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        assertFalse(indexableFields.isEmpty());
        assertTrue(indexableFields.size() >= geometry.size());
    }

    public void testIndexingRectangle() {
        Rectangle geometry = randomRectangle();
        final List<IndexableField> indexableFields = indexer.indexShape(parseContext, geometry);
        assertNotNull(indexableFields);
        assertFalse(indexableFields.isEmpty());
    }

    public void testPrepareForIndexingCircle() {
        Circle circle = randomCircle();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> indexer.prepareForIndexing(circle)
        );
        assertEquals("CIRCLE is not supported", exception.getMessage());
    }

    public void testPrepareForIndexingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES));
        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> indexer.prepareForIndexing(ring));
        assertEquals("cannot index LINEARRING [ " + ring + " ] directly", exception.getMessage());
    }

    public void testPrepareForIndexingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit);
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit);
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingPoint() {
        Point geometry = randomPoint();
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit);
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingPolygon() throws IOException, ParseException {
        Polygon geometry = randomPolygon();
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingMultiPolygon() throws IOException, ParseException {
        MultiPolygon geometry = randomMultiPolygon();
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingGeometryCollection() throws IOException, ParseException {
        GeometryCollection geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testPrepareForIndexingRectangle() {
        Rectangle geometry = randomRectangle();
        assertEquals(geometry, indexer.prepareForIndexing(geometry));
    }

    public void testProcessedClass() {
        assertEquals(Geometry.class, indexer.processedClass());
    }

}
