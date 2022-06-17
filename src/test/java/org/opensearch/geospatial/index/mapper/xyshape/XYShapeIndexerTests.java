/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

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
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.test.OpenSearchTestCase;

public class XYShapeIndexerTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;
    private XYShapeIndexer indexer;
    private GeometryVisitor<IndexableField[], RuntimeException> mockIndexableFieldVisitor;
    private GeometryVisitor<Geometry, RuntimeException> mockSupportVisitor;
    private ParseContext parseContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockIndexableFieldVisitor = mock(GeometryVisitor.class);
        mockSupportVisitor = mock(GeometryVisitor.class);
        indexer = new XYShapeIndexer(mockSupportVisitor, mockIndexableFieldVisitor);
        parseContext = mock(ParseContext.class);
    }

    public void testIndexingNullGeometry() {
        expectThrows(NullPointerException.class, () -> indexer.indexShape(parseContext, null));
    }

    public void testIndexingCircle() {
        Circle circle = randomCircle();
        when(mockIndexableFieldVisitor.visit(circle)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, circle));
        verify(mockIndexableFieldVisitor).visit(circle);
    }

    public void testIndexingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES));
        when(mockIndexableFieldVisitor.visit(ring)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, ring));
        verify(mockIndexableFieldVisitor).visit(ring);
    }

    public void testIndexingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit);
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit);
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingPoint() {
        Point geometry = randomPoint();
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit);
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingPolygon() throws IOException, ParseException {
        Polygon geometry = randomPolygon();
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingMultiPolygon() throws IOException, ParseException {
        MultiPolygon geometry = randomMultiPolygon();
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingGeometryCollection() {
        GeometryCollection<Geometry> geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testIndexingRectangle() {
        Rectangle geometry = randomRectangle();
        when(mockIndexableFieldVisitor.visit(geometry)).thenReturn(new IndexableField[0]);
        assertNotNull("failed to index geometry", indexer.indexShape(parseContext, geometry));
        verify(mockIndexableFieldVisitor).visit(geometry);
    }

    public void testPrepareIndexingCircle() {
        Circle circle = randomCircle();
        indexer.prepareForIndexing(circle);
        verify(mockSupportVisitor).visit(circle);
    }

    public void testPrepareIndexingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES));
        indexer.prepareForIndexing(ring);
        verify(mockSupportVisitor).visit(ring);
    }

    public void testPrepareIndexingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line geometry = randomLine(verticesLimit);
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine geometry = randomMultiLine(verticesLimit, linesLimit);
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingPoint() {
        Point geometry = randomPoint();
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint geometry = randomMultiPoint(pointLimit);
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingPolygon() throws IOException, ParseException {
        Polygon geometry = randomPolygon();
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingMultiPolygon() throws IOException, ParseException {
        MultiPolygon geometry = randomMultiPolygon();
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingGeometryCollection() {
        GeometryCollection<Geometry> geometry = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testPrepareIndexingRectangle() {
        Rectangle geometry = randomRectangle();
        indexer.prepareForIndexing(geometry);
        verify(mockSupportVisitor).visit(geometry);
    }

    public void testProcessedClass() {
        assertEquals(Geometry.class, indexer.processedClass());
    }

}
