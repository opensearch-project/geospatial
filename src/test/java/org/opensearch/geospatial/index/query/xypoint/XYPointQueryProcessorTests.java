/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xypoint;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomCircle;
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
import org.opensearch.common.geo.ShapeRelation;
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
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.test.OpenSearchTestCase;

public class XYPointQueryProcessorTests extends OpenSearchTestCase {
    private XYPointQueryProcessor queryProcessor;
    private String fieldName;
    private ShapeRelation relation;
    private QueryShardContext context;
    private static final boolean VALID_FIELD_TYPE = true;
    private static final boolean INVALID_FIELD_TYPE = false;
    private final static Integer MAX_NUMBER_OF_VERTICES = 10;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static Integer MIN_NUMBER_OF_GEOMETRY_OBJECTS = 10;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        context = mock(QueryShardContext.class);
        fieldName = GeospatialTestHelper.randomLowerCaseString();
        relation = ShapeRelation.INTERSECTS;
        queryProcessor = new XYPointQueryProcessor();
    }

    public void testInvalidRelation() {
        mockFieldType(VALID_FIELD_TYPE);
        QueryShardException exception = expectThrows(
            QueryShardException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, ShapeRelation.CONTAINS, context)
        );
        assertTrue(exception.getMessage().contains("[CONTAINS] query relation not supported"));
    }

    public void testQueryingNullFieldName() {
        assertThrows(NullPointerException.class, () -> queryProcessor.shapeQuery(randomPolygon(), null, relation, context));
    }

    public void testQueryingNullQueryContext() {
        assertThrows(NullPointerException.class, () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, relation, null));
    }

    public void testQueryingNullShapeRelation() {
        mockFieldType(VALID_FIELD_TYPE);
        QueryShardException exception = expectThrows(
            QueryShardException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, null, context)
        );
        assertTrue(exception.getMessage().contains("[null] query relation not supported"));
    }

    public void testQueryingNullGeometry() {
        mockFieldType(VALID_FIELD_TYPE);
        assertThrows(NullPointerException.class, () -> queryProcessor.shapeQuery(null, fieldName, relation, context));
    }

    public void testQueryingEmptyGeometry() {
        mockFieldType(VALID_FIELD_TYPE);
        final Query query = queryProcessor.shapeQuery(new GeometryCollection<>(), fieldName, relation, context);
        assertEquals("No match found query should be returned", new MatchNoDocsQuery(), query);
    }

    public void testQueryingInvalidFieldTypeGeometry() {
        mockFieldType(INVALID_FIELD_TYPE);
        final QueryShardException exception = expectThrows(
            QueryShardException.class,
            () -> queryProcessor.shapeQuery(randomPolygon(), fieldName, relation, context)
        );
        assertEquals(
            "wrong exception message",
            String.format(
                Locale.ROOT,
                "Expected [%s] field type for Field [%s] but found [geo_point]",
                XYPointFieldMapper.CONTENT_TYPE,
                fieldName
            ),
            exception.getMessage()
        );
    }

    public void testQueryingLinearRing() {
        mockFieldType(VALID_FIELD_TYPE);
        LinearRing ring = randomLinearRing(randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES), randomBoolean());
        expectThrows(QueryShardException.class, () -> queryProcessor.shapeQuery(ring, fieldName, relation, context));
    }

    public void testQueryingMultiLine() {
        mockFieldType(VALID_FIELD_TYPE);
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        final int linesLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiLine multiLine = randomMultiLine(verticesLimit, linesLimit, randomBoolean());
        expectThrows(QueryShardException.class, () -> queryProcessor.shapeQuery(multiLine, fieldName, relation, context));
    }

    public void testQueryingMultiPoint() {
        mockFieldType(VALID_FIELD_TYPE);
        int pointLimit = atLeast(MIN_NUMBER_OF_GEOMETRY_OBJECTS);
        MultiPoint multiPoint = randomMultiPoint(pointLimit, randomBoolean());
        expectThrows(QueryShardException.class, () -> queryProcessor.shapeQuery(multiPoint, fieldName, relation, context));
    }

    public void testQueryingPoint() {
        mockFieldType(VALID_FIELD_TYPE);
        Point point = randomPoint(randomBoolean());
        expectThrows(QueryShardException.class, () -> queryProcessor.shapeQuery(point, fieldName, relation, context));
    }

    public void testQueryingLine() {
        mockFieldType(VALID_FIELD_TYPE);
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line line = randomLine(verticesLimit, randomBoolean());
        expectThrows(QueryShardException.class, () -> queryProcessor.shapeQuery(line, fieldName, relation, context));
    }

    public void testQueryingCircle() {
        mockFieldType(VALID_FIELD_TYPE);
        Circle circle = randomCircle(randomBoolean());
        assertNotNull("failed to convert to Query", queryProcessor.shapeQuery(circle, fieldName, relation, context));
    }

    public void testQueryingRectangle() {
        mockFieldType(VALID_FIELD_TYPE);
        Rectangle rectangle = randomRectangle();
        assertNotNull("failed to convert to Query", queryProcessor.shapeQuery(rectangle, fieldName, relation, context));
    }

    public void testQueryingPolygon() throws IOException, ParseException {
        mockFieldType(VALID_FIELD_TYPE);
        Polygon geometry = randomPolygon();
        assertNotNull("failed to convert to Query", queryProcessor.shapeQuery(geometry, fieldName, relation, context));
    }

    public void testQueryingMultiPolygon() throws IOException, ParseException {
        mockFieldType(VALID_FIELD_TYPE);
        MultiPolygon geometry = randomMultiPolygon();
        assertNotNull("failed to convert to Query", queryProcessor.shapeQuery(geometry, fieldName, relation, context));
    }

    public void testQueryingEmptyGeometryCollection() {
        mockFieldType(VALID_FIELD_TYPE);
        GeometryCollection<Geometry> geometry = new GeometryCollection<>();
        final Query actualQuery = queryProcessor.shapeQuery(geometry, fieldName, relation, context);
        assertNotNull("failed to convert to Query", actualQuery);
        assertEquals("MatchNoDocs query should be returned", new MatchNoDocsQuery(), actualQuery);
    }

    public void testGetVectorQueryFromShape() {
        mockFieldType(VALID_FIELD_TYPE);
        Circle circle = randomCircle(randomBoolean());
        assertNotNull("failed to convert to Query", queryProcessor.shapeQuery(circle, fieldName, relation, context));
    }

    private void mockFieldType(boolean success) {
        if (success) {
            when(context.fieldMapper(fieldName)).thenReturn(
                new XYPointFieldMapper.XYPointFieldType(
                    fieldName,
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    emptyMap(),
                    mock(XYPointQueryProcessor.class)
                )
            );
            return;
        }
        when(context.fieldMapper(fieldName)).thenReturn(new GeoPointFieldMapper.GeoPointFieldType(fieldName));
    }
}
