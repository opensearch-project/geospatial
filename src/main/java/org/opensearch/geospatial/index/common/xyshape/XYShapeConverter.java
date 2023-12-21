/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.xyshape;

import static org.apache.commons.lang3.ArrayUtils.toPrimitive;

import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Utility class to convert compatible shapes from opensearch to Lucene
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class XYShapeConverter {

    /**
     * Convert from {@link Line} to {@link XYLine}
     * @param line of type {@link Line}
     * @return {@link XYLine} instance
     */
    public static XYLine toXYLine(@NonNull Line line) {
        var x = toFloatArray(line.getX());
        var y = toFloatArray(line.getY());
        return new XYLine(x, y);
    }

    /**
     * Convert from {@link Rectangle} to {@link XYPolygon}
     * @param rectangle of type {@link Rectangle}
     * @return {@link XYPolygon} instance
     */
    public static XYPolygon toXYPolygon(@NonNull Rectangle rectangle) {
        // build polygon by assigning points in Counter Clock Wise direction (default for polygon) and end at where
        // you started since it has to be linear ring
        // (minX,minY) -> (maxX, minY) -> (maxX, maxY) -> (minX, maxY) -> (minX, minY)
        var x = new double[] { rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMaxX(), rectangle.getMinX(), rectangle.getMinX() };
        var y = new double[] { rectangle.getMinY(), rectangle.getMinY(), rectangle.getMaxY(), rectangle.getMaxY(), rectangle.getMinY() };
        return new XYPolygon(toFloatArray(x), toFloatArray(y));
    }

    /**
     * Convert from {@link Polygon} to {@link XYPolygon}
     * @param polygon of type {@link Polygon}
     * @return {@link XYPolygon} instance
     */
    public static XYPolygon toXYPolygon(@NonNull Polygon polygon) {
        XYPolygon[] holes = extractXYPolygonsFromHoles(polygon);
        Line line = polygon.getPolygon();
        XYLine polygonLine = toXYLine(line);
        return new XYPolygon(polygonLine.getX(), polygonLine.getY(), holes);
    }

    /**
     * Convert from {@link Circle} to {@link XYCircle}
     * @param circle of type {@link Circle}
     * @return {@link XYCircle} instance
     */
    public static XYCircle toXYCircle(@NonNull Circle circle) {
        return new XYCircle(
            Double.valueOf(circle.getX()).floatValue(),
            Double.valueOf(circle.getY()).floatValue(),
            Double.valueOf(circle.getRadiusMeters()).floatValue()
        );
    }

    /**
     * Convert from {@link Point} to {@link XYPoint}
     * @param point of type {@link Point}
     * @return {@link XYPoint} instance
     */
    public static XYPoint toXYPoint(@NonNull Point point) {
        var x = Double.valueOf(point.getX()).floatValue();
        var y = Double.valueOf(point.getY()).floatValue();
        return new XYPoint(x, y);
    }

    /**
     * Convert from {@link Rectangle} to {@link XYRectangle}
     * @param rectangle of type {@link Rectangle}
     * @return {@link XYRectangle} instance
     */
    public static XYRectangle toXYRectangle(@NonNull Rectangle rectangle) {
        var minX = Double.valueOf(rectangle.getMinX()).floatValue();
        var maxX = Double.valueOf(rectangle.getMaxX()).floatValue();
        var minY = Double.valueOf(rectangle.getMinY()).floatValue();
        var maxY = Double.valueOf(rectangle.getMaxY()).floatValue();
        return new XYRectangle(minX, maxX, minY, maxY);
    }

    // Each hole inside a polygon is just a polygon inside the polygon itself. This method will
    // extract all the holes into list of individual polygons.
    private static XYPolygon[] extractXYPolygonsFromHoles(final Polygon polygon) {
        return IntStream.range(0, polygon.getNumberOfHoles())
            .mapToObj(polygon::getHole)
            .map(XYShapeConverter::toXYLine)
            .map(line -> new XYPolygon(line.getX(), line.getY()))
            .collect(Collectors.toList())
            .toArray(XYPolygon[]::new);

    }

    private static float[] toFloatArray(double[] input) {
        final var floatRefArray = DoubleStream.of(input).boxed().map(Double::floatValue).toArray(Float[]::new);
        return toPrimitive(floatRefArray);
    }
}
