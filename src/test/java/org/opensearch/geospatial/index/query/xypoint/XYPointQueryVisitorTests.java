/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xypoint;

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
import java.util.Locale;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
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
import org.opensearch.geometry.ShapeType;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.test.OpenSearchTestCase;

public class XYPointQueryVisitorTests extends OpenSearchTestCase {
    private final static Integer MAX_NUMBER_OF_VERTICES = 10;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;
    private GeometryVisitor<Query, RuntimeException> queryVisitor;
    private String fieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        QueryShardContext context = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(XYPointFieldMapper.XYPointFieldType.class);
        fieldName = GeospatialTestHelper.randomLowerCaseString();
        queryVisitor = new XYPointQueryVisitor(context, fieldType, fieldName);
    }

    public void testQueryingLinearRing() {
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES), randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> ring.visit(queryVisitor));
        assertEquals(
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape [%s]", fieldName, ShapeType.LINEARRING.name()),
            exception.getMessage()
        );
    }

    public void testQueryingMultiLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine multiLine = randomMultiLine(verticesLimit, linesLimit, randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> multiLine.visit(queryVisitor));
        assertEquals(
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape [%s]", fieldName, ShapeType.MULTILINESTRING.name()),
            exception.getMessage()
        );
    }

    public void testQueryingMultiPoint() {
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint multiPoint = randomMultiPoint(pointLimit, randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> multiPoint.visit(queryVisitor));
        assertEquals(
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape [%s]", fieldName, ShapeType.MULTIPOINT.name()),
            exception.getMessage()
        );
    }

    public void testQueryingPoint() {
        Point point = randomPoint(randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> point.visit(queryVisitor));
        assertEquals(
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape [%s]", fieldName, ShapeType.POINT.name()),
            exception.getMessage()
        );
    }

    public void testQueryingLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line line = randomLine(verticesLimit, randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> line.visit(queryVisitor));
        assertEquals(
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape [%s]", fieldName, ShapeType.LINESTRING.name()),
            exception.getMessage()
        );
    }

    public void testQueryingCircle() {
        Circle circle = randomCircle(randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> circle.visit(queryVisitor));
        assertEquals(
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape [%s]", fieldName, ShapeType.CIRCLE.name()),
            exception.getMessage()
        );
    }

    public void testQueryingRectangle() {
        Rectangle rectangle = randomRectangle();
        Query query = rectangle.visit(queryVisitor);
        assertNotNull("failed to convert to Query", query);
    }

    public void testQueryingPolygon() throws IOException, ParseException {
        Polygon polygon = randomPolygon();
        Query query = polygon.visit(queryVisitor);
        assertNotNull("failed to convert to Query", query);
    }

    public void testQueryingMultiPolygon() throws IOException, ParseException {
        MultiPolygon multiPolygon = randomMultiPolygon();
        Query query = multiPolygon.visit(queryVisitor);
        assertNotNull("failed to convert to Query", query);
    }

    public void testQueryingEmptyGeometryCollection() {
        GeometryCollection<?> collection = new GeometryCollection<>();
        Query query = collection.visit(queryVisitor);
        assertNotNull("failed to convert to Query", query);
        assertEquals("MatchNoDocs query should be returned", new MatchNoDocsQuery(), query);
    }

    public void testQueryingUnsupportedGeometryCollection() {
        GeometryCollection<?> collection = randomGeometryCollection(MIN_NUMBER_OF_GEOMETRY_OBJECTS, randomBoolean());
        QueryShardException exception = expectThrows(QueryShardException.class, () -> collection.visit(queryVisitor));
        assertTrue(
            "Validation failed for unsupported geometries",
            exception.getMessage().contains(String.format(Locale.ROOT, "Field [%s] found an unsupported shape", fieldName))
        );
    }
}
