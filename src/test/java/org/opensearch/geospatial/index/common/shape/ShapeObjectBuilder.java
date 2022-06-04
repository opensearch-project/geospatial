/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.shape;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.opensearch.common.Randomness;
import org.opensearch.geo.GeometryTestUtils;
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
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

// Class to build various Geometry shapes for unit tests.
public class ShapeObjectBuilder {

    public static double[] generateRandomDoubleArray(int maxArraySize) {
        if (maxArraySize < 1) {
            throw new IllegalArgumentException("size cannot be less than 1");
        }
        return Randomness.get().doubles(maxArraySize).toArray();
    }

    public static Circle randomCircle() {
        return new Circle(randomDouble(), randomDouble(), randomDouble());
    }

    public static LinearRing randomLinearRing(int size) {
        double[] x = generateRandomDoubleArray(size);
        x[0] = x[size - 1];
        double[] y = generateRandomDoubleArray(size);
        y[0] = y[size - 1];
        double[] z = generateRandomDoubleArray(size);
        z[0] = z[size - 1];
        return new LinearRing(x, y, z);
    }

    public static Point randomPoint() {
        return new Point(randomDouble(), randomDouble(), randomDouble());
    }

    public static MultiPoint randomMultiPoint(int maximumPoints) {
        final List<Point> points = IntStream.range(0, maximumPoints).mapToObj(unUsed -> randomPoint()).collect(Collectors.toList());
        return new MultiPoint(points);
    }

    public static Line randomLine(int size) {
        if (size < 2) {
            throw new IllegalArgumentException("atleast two points are required");
        }
        double[] x = generateRandomDoubleArray(size);
        double[] y = generateRandomDoubleArray(size);
        double[] z = generateRandomDoubleArray(size);
        return new Line(x, y, z);
    }

    public static MultiLine randomMultiLine(int size, int maximumLines) {
        final List<Line> lines = IntStream.range(0, maximumLines).mapToObj(unUsed -> randomLine(size)).collect(Collectors.toList());
        return new MultiLine(lines);
    }

    // Generating truly random polygon is complicated, hence, will be randomly selecting from with holes, without holes and geo-polygon
    public static Polygon randomPolygon() throws IOException, ParseException {
        return (Polygon) RandomPicks.randomFrom(
            Randomness.get(),
            List.of(getPolygonWithHoles(), getPolygonWithoutHoles(), GeometryTestUtils.randomPolygon(randomBoolean()))
        );
    }

    public static MultiPolygon randomMultiPolygon() throws IOException, ParseException {
        return (MultiPolygon) RandomPicks.randomFrom(
            Randomness.get(),
            List.of(getMultiPolygon(), GeometryTestUtils.randomMultiPolygon(randomBoolean()))
        );
    }

    private static Geometry getPolygonWithHoles() throws IOException, ParseException {
        return WellKnownText.INSTANCE.fromWKT(
            "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))"
        );
    }

    private static Geometry getMultiPolygon() throws IOException, ParseException {
        return WellKnownText.INSTANCE.fromWKT("MULTIPOLYGON( ((10 10, 10 20, 20 20, 20 15, 10 10)), ((60 60, 70 70, 80 60, 60 60 )) )");
    }

    private static Geometry getPolygonWithoutHoles() throws IOException, ParseException {
        return WellKnownText.INSTANCE.fromWKT("POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))");
    }

    private static Geometry randomGeometry(int size) throws IOException, ParseException {
        return RandomPicks.randomFrom(Randomness.get(), List.of(randomLine(size), randomMultiPoint(size), randomPoint()));
    }

    public static GeometryCollection randomGeometryCollection(int maximum) throws IOException, ParseException {
        // Test at leaset 2 items in collection
        int size = OpenSearchTestCase.randomIntBetween(2, maximum);
        List<Geometry> shapes = new ArrayList();
        for (int count = 0; count < size; count++) {
            shapes.add(randomGeometry(size));
        }
        return new GeometryCollection(shapes);
    }

    public static Rectangle randomRectangle() {
        double x[] = randomPointsForRectangle();
        double y[] = randomPointsForRectangle();
        return new Rectangle(x[0], x[1], y[1], y[0]);
    }

    private static double[] randomPointsForRectangle() {
        double first = randomDouble();
        double second = randomDouble();
        while (first == second) {
            second = randomDouble();
        }
        double vertex[] = new double[2];
        vertex[0] = Math.min(first, second);
        vertex[1] = Math.max(first, second);
        return vertex;
    }

}
