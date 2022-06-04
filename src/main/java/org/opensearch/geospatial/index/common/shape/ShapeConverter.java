/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.shape;

import static org.apache.commons.lang3.ArrayUtils.toPrimitive;

import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.ShapeType;

// Utility class to convert compatible shapes from opensearch to Lucene
public class ShapeConverter {

    public static XYLine toXYLine(Line line) {
        Objects.requireNonNull(line, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.LINESTRING));
        float[] x = toFloatArray(line.getX());
        float[] y = toFloatArray(line.getY());
        return new XYLine(x, y);
    }

    public static XYPolygon toXYPolygon(Rectangle rectangle) {
        Objects.requireNonNull(rectangle, "Rectangle cannot be null");
        // build polygon by assigning points in Counter Clock Wise direction (default for polygon) and end at where
        // you started since it has to be linear ring
        // (minX,minY) -> (maxX, minY) -> (maxX, maxY) -> (minX, maxY) -> (minX, minY)
        double[] x = new double[] {
            rectangle.getMinX(),
            rectangle.getMaxX(),
            rectangle.getMaxX(),
            rectangle.getMinX(),
            rectangle.getMinX() };
        double[] y = new double[] {
            rectangle.getMinY(),
            rectangle.getMinY(),
            rectangle.getMaxY(),
            rectangle.getMaxY(),
            rectangle.getMinY() };
        return new XYPolygon(toFloatArray(x), toFloatArray(y));
    }

    public static XYPolygon toXYPolygon(Polygon polygon) {
        Objects.requireNonNull(polygon, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.POLYGON));
        XYPolygon[] holes = extractXYPolygonsFromHoles(polygon);
        Line line = polygon.getPolygon();
        XYLine polygonLine = toXYLine(line);
        return new XYPolygon(polygonLine.getX(), polygonLine.getY(), holes);
    }

    // Each hole inside a polygon is just a polygon inside the polygon itself. This method will
    // extract all the holes into list of individual polygons.
    private static XYPolygon[] extractXYPolygonsFromHoles(final Polygon polygon) {
        return IntStream.range(0, polygon.getNumberOfHoles())
            .mapToObj(polygon::getHole)
            .map(ShapeConverter::toXYLine)
            .map(line -> new XYPolygon(line.getX(), line.getY()))
            .collect(Collectors.toList())
            .toArray(XYPolygon[]::new);

    }

    private static float[] toFloatArray(double[] input) {
        final Float[] floats = DoubleStream.of(input).boxed().map(Double::floatValue).toArray(Float[]::new);
        return toPrimitive(floats);
    }
}
