/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.shape;

import static org.opensearch.geospatial.GeospatialTestHelper.toDoubleArray;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomLine;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomPolygon;
import static org.opensearch.geospatial.index.common.shape.ShapeObjectBuilder.randomRectangle;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.test.OpenSearchTestCase;

public class ShapeConverterTests extends OpenSearchTestCase {

    private final static Integer MAX_NUMBER_OF_VERTICES = 100;
    private final static Integer MIN_NUMBER_OF_VERTICES = 2;
    private final static double DELTA_ERROR = 0.1;

    public void testXYLine() {
        int verticesLimit = randomIntBetween(MIN_NUMBER_OF_VERTICES, MAX_NUMBER_OF_VERTICES);
        Line line = randomLine(verticesLimit);
        final XYLine xyLine = ShapeConverter.toXYLine(line);
        assertArrayEquals("not matching x coords", line.getX(), toDoubleArray(xyLine.getX()), DELTA_ERROR);
        assertArrayEquals("not matching y coords", line.getY(), toDoubleArray(xyLine.getY()), DELTA_ERROR);
    }

    public void testRectangleToXYPolygon() {
        Rectangle rectangle = randomRectangle();
        final XYPolygon xyPolygon = ShapeConverter.toXYPolygon(rectangle);
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
        final XYPolygon xyPolygon = ShapeConverter.toXYPolygon(polygon);
        assertNotNull("failed to convert to XYPolygon", xyPolygon);
        assertArrayEquals(polygon.getPolygon().getX(), toDoubleArray(xyPolygon.getPolyX()), DELTA_ERROR);
        assertArrayEquals(polygon.getPolygon().getY(), toDoubleArray(xyPolygon.getPolyY()), DELTA_ERROR);
        assertEquals("number of holes are  differnt", polygon.getNumberOfHoles(), xyPolygon.numHoles());
    }
}
