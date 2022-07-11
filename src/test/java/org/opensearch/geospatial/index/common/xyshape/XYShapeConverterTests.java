/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.xyshape;

import static org.opensearch.geospatial.GeospatialTestHelper.toDoubleArray;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomLine;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomPoint;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomPolygon;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.randomRectangle;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.test.OpenSearchTestCase;

public class XYShapeConverterTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static double DELTA_ERROR = 0.1;

    public void testXYLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line line = randomLine(verticesLimit, randomBoolean());
        final XYLine xyLine = XYShapeConverter.toXYLine(line);
        assertArrayEquals("not matching x coords", line.getX(), toDoubleArray(xyLine.getX()), DELTA_ERROR);
        assertArrayEquals("not matching y coords", line.getY(), toDoubleArray(xyLine.getY()), DELTA_ERROR);
    }

    public void testRectangleToXYPolygon() {
        Rectangle rectangle = randomRectangle();
        final XYPolygon xyPolygon = XYShapeConverter.toXYPolygon(rectangle);
        assertNotNull("failed to convert to XYPolygon", xyPolygon);
        Double[] expectedXCoords = Arrays.asList(
            rectangle.getMinX(),
            rectangle.getMaxX(),
            rectangle.getMaxX(),
            rectangle.getMinX(),
            rectangle.getMinX()
        ).toArray(Double[]::new);
        Double[] expectedYCoords = Arrays.asList(
            rectangle.getMinY(),
            rectangle.getMinY(),
            rectangle.getMaxY(),
            rectangle.getMaxY(),
            rectangle.getMinY()
        ).toArray(Double[]::new);
        final double[] actualXCoords = toDoubleArray(xyPolygon.getPolyX());
        final double[] actualYCoords = toDoubleArray(xyPolygon.getPolyY());
        assertArrayEquals(ArrayUtils.toPrimitive(expectedXCoords), actualXCoords, DELTA_ERROR);
        assertArrayEquals(ArrayUtils.toPrimitive(expectedYCoords), actualYCoords, DELTA_ERROR);

    }

    public void testPolygonToXYPolygon() throws IOException, ParseException {
        Polygon polygon = randomPolygon();
        final XYPolygon xyPolygon = XYShapeConverter.toXYPolygon(polygon);
        assertNotNull("failed to convert to XYPolygon", xyPolygon);
        assertArrayEquals(polygon.getPolygon().getX(), toDoubleArray(xyPolygon.getPolyX()), DELTA_ERROR);
        assertArrayEquals(polygon.getPolygon().getY(), toDoubleArray(xyPolygon.getPolyY()), DELTA_ERROR);
        assertEquals("number of holes are  differnt", polygon.getNumberOfHoles(), xyPolygon.numHoles());
    }

    public void testRectangleToXYRectangle() {
        Rectangle rectangle = randomRectangle();
        final XYRectangle xyRectangle = XYShapeConverter.toXYRectangle(rectangle);
        assertNotNull("failed to convert to XYRectangle", xyRectangle);
        assertArrayEquals(
            "Vertices didn't match between Rectangle and XYRectangle",
            new double[] { rectangle.getMaxX(), rectangle.getMaxY(), rectangle.getMinX(), rectangle.getMinY() },
            toDoubleArray(new float[] { xyRectangle.maxX, xyRectangle.maxY, xyRectangle.minX, xyRectangle.minY }),
            DELTA_ERROR
        );
    }

    public void testXYPoint() {
        Point point = randomPoint(randomBoolean());
        final XYPoint xyPoint = XYShapeConverter.toXYPoint(point);
        assertArrayEquals(
            "Coordinates didn't match",
            new double[] { point.getX(), point.getY() },
            toDoubleArray(new float[] { xyPoint.getX(), xyPoint.getY() }),
            DELTA_ERROR
        );
    }
}
