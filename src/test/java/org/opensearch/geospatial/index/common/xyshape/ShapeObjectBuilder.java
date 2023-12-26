/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.xyshape;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomValueOtherThanMany;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.opensearch.common.Randomness;
import org.opensearch.common.geo.ShapeRelation;
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
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ShapeTestUtil;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import lombok.SneakyThrows;

// Class to build various Geometry shapes for unit tests.
public class ShapeObjectBuilder {

    public static final int BEGIN = 0;
    private static final int MIN_VERTEX = 3;
    private static final int MAX_TOTAL_VERTEX = 10;
    public static final int MAX_ATTEMPT_TO_RETURN_GEOMETRY = 5;

    public static double[] generateRandomDoubleArray(int maxArraySize) {
        if (maxArraySize < 1) {
            throw new IllegalArgumentException("size cannot be less than 1");
        }
        return Randomness.get().doubles(maxArraySize).toArray();
    }

    public static Circle randomCircle(boolean hasZCoords) {
        double x = randomDouble();
        double y = randomDouble();
        double radius = randomDouble();
        if (!hasZCoords) {
            return new Circle(x, y, radius);
        }
        double z = randomDouble();
        return new Circle(x, y, z, radius);
    }

    public static LinearRing randomLinearRing(int size, boolean hasZCoords) {
        double[] x = generateRandomDoubleArray(size);
        x[0] = x[size - 1];
        double[] y = generateRandomDoubleArray(size);
        y[0] = y[size - 1];
        if (!hasZCoords) {
            return new LinearRing(x, y);
        }
        double[] z = generateRandomDoubleArray(size);
        z[0] = z[size - 1];
        return new LinearRing(x, y, z);
    }

    public static Point randomPoint(boolean hasZCoords) {
        double x = randomDouble();
        double y = randomDouble();
        if (!hasZCoords) {
            return new Point(x, y);
        }
        double z = randomDouble();
        return new Point(x, y, z);
    }

    public static MultiPoint randomMultiPoint(int maximumPoints, boolean hasZCoords) {
        final List<Point> points = IntStream.range(BEGIN, maximumPoints)
            .mapToObj(unUsed -> randomPoint(hasZCoords))
            .collect(Collectors.toList());
        return new MultiPoint(points);
    }

    public static Line randomLine(int size, boolean hasZCoords) {
        if (size < 2) {
            throw new IllegalArgumentException("atleast two points are required");
        }
        double[] x = generateRandomDoubleArray(size);
        double[] y = generateRandomDoubleArray(size);
        if (!hasZCoords) {
            return new Line(x, y);
        }
        double[] z = generateRandomDoubleArray(size);
        return new Line(x, y, z);
    }

    public static MultiLine randomMultiLine(int size, int maximumLines, boolean hasZCoords) {
        final List<Line> lines = IntStream.range(BEGIN, maximumLines)
            .mapToObj(unUsed -> randomLine(size, hasZCoords))
            .collect(Collectors.toList());
        return new MultiLine(lines);
    }

    /**
     * Generate polygon in double range
     *
     * Generating truly random polygon is complicated, hence, will be randomly selecting from with holes, without holes and geo-polygon
     * If you try to index the polygon returned by this method, it might fail because
     * polygon point will be cast to float type and lose its original value to form a correct polygon.
     * Use {@link #randomXYPolygon()} if you need to index a polygon.
     *
     * @return Randomly generated polygon in double range
     * @throws IOException
     * @throws ParseException
     */
    public static Polygon randomPolygon() throws IOException, ParseException {
        return (Polygon) RandomPicks.randomFrom(
            Randomness.get(),
            // TODO: Support z coordinates to be added to polygon
            List.of(getPolygonWithHoles(), getPolygonWithoutHoles(), GeometryTestUtils.randomPolygon(false))
        );
    }

    /**
     * Generate polygon in float range
     *
     * Generating truly random polygon is complicated, hence, will be randomly selecting from with holes, without holes and geo-polygon
     * You need to use this method to test indexing polygon as lucene XYPolygon support only float range.
     *
     * @return Randomly generated polygon in float range
     * @throws IOException
     * @throws ParseException
     */
    public static Polygon randomXYPolygon() throws IOException, ParseException {
        return (Polygon) RandomPicks.randomFrom(
            Randomness.get(),
            // TODO: Support z coordinates to be added to polygon
            List.of(getPolygonWithHoles(), getPolygonWithoutHoles(), validRandomXYPolygon(false))
        );
    }

    /**
     * Generate multi polygon in double range
     *
     * If you try to index the multi polygon returned by this method, it might fail because
     * polygon point will be cast to float type and lose its original value to form a correct polygon.
     *
     * Use {@link #randomMultiXYPolygon()} if you need to index a multi polygon.
     *
     * @return Randomly generated multi polygon in double range
     * @throws IOException
     * @throws ParseException
     */
    public static MultiPolygon randomMultiPolygon() throws IOException, ParseException {
        return (MultiPolygon) RandomPicks.randomFrom(
            Randomness.get(),
            // TODO: Support z coordinates to be added to Multi polygon
            List.of(getMultiPolygon(), GeometryTestUtils.randomMultiPolygon(false))
        );
    }

    /**
     * Generate multi polygon in float range
     *
     * You need to use this method to test indexing multi polygon as lucene XYPolygon support only float range.
     *
     * @return Randomly generated multi polygon in float range
     * @throws IOException
     * @throws ParseException
     */
    public static MultiPolygon randomMultiXYPolygon() throws IOException, ParseException {
        return (MultiPolygon) RandomPicks.randomFrom(
            Randomness.get(),
            // TODO: Support z coordinates to be added to Multi polygon
            List.of(getMultiPolygon(), randomMultiXYPolygon(false))
        );
    }

    /**
     * Copied from {@code org.opensearch.geo.GeometryTestUtils#randomMultiPolygon} with changes
     * from calling {@code org.opensearch.geo.GeometryTestUtils#randomPolygon} to
     * calling {@code org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder#randomXyPolygon}
     */
    private static MultiPolygon randomMultiXYPolygon(final boolean hasAlt) {
        int size = OpenSearchTestCase.randomIntBetween(3, 10);
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            polygons.add(validRandomXYPolygon(hasAlt));
        }
        return new MultiPolygon(polygons);
    }

    /**
     * Temporary method to avoid invalid polygon returned by {@link #randomXYPolygon}
     * @param hasAlt
     * @return
     */
    // TODO: Implement randomXYPolygon to return only valid polygon and remove this method
    @SneakyThrows
    private static Polygon validRandomXYPolygon(boolean hasAlt) {
        Polygon polygon = randomXYPolygon(hasAlt);
        try {
            XYPolygon xyPolygon = XYShapeConverter.toXYPolygon(polygon);
            // Test validity of polygon
            Tessellator.tessellate(xyPolygon, true);
            return polygon;
        } catch (Exception e) {
            return (Polygon) RandomPicks.randomFrom(Randomness.get(), List.of(getPolygonWithHoles(), getPolygonWithoutHoles()));
        }
    }

    /**
     * Copied from {@code org.opensearch.geo.GeometryTestUtils#randomPolygon} with changes
     * from {@code org.apache.lucene.geo.Polygon} to {@code org.apache.lucene.geo.XYPolygon}
     */
    private static Polygon randomXYPolygon(boolean hasAlt) {
        org.apache.lucene.geo.XYPolygon luceneXYPolygon = randomValueOtherThanMany(p -> area(p) == 0, ShapeTestUtil::nextPolygon);
        if (luceneXYPolygon.numHoles() > 0) {
            org.apache.lucene.geo.XYPolygon[] luceneHoles = luceneXYPolygon.getHoles();
            List<LinearRing> holes = new ArrayList<>();
            for (int i = 0; i < luceneXYPolygon.numHoles(); i++) {
                org.apache.lucene.geo.XYPolygon xyPoly = luceneHoles[i];
                holes.add(
                    GeometryTestUtils.linearRing(
                        GeospatialTestHelper.toDoubleArray(xyPoly.getPolyX()),
                        GeospatialTestHelper.toDoubleArray(xyPoly.getPolyY()),
                        hasAlt
                    )
                );
            }
            return new Polygon(
                GeometryTestUtils.linearRing(
                    GeospatialTestHelper.toDoubleArray(luceneXYPolygon.getPolyX()),
                    GeospatialTestHelper.toDoubleArray(luceneXYPolygon.getPolyY()),
                    hasAlt
                ),
                holes
            );
        }
        return new Polygon(
            GeometryTestUtils.linearRing(
                GeospatialTestHelper.toDoubleArray(luceneXYPolygon.getPolyX()),
                GeospatialTestHelper.toDoubleArray(luceneXYPolygon.getPolyY()),
                hasAlt
            )
        );
    }

    /**
     * Copied from {@code org.opensearch.geo.GeometryTestUtils#area} with changes
     * from {@code org.apache.lucene.geo.Polygon} to {@code org.apache.lucene.geo.XYPolygon}
     */
    private static double area(org.apache.lucene.geo.XYPolygon luceneXYPolygon) {
        double windingSum = 0;
        final int numPts = luceneXYPolygon.numPoints() - 1;
        for (int i = 0; i < numPts; i++) {
            // compute signed area
            windingSum += luceneXYPolygon.getPolyX(i) * luceneXYPolygon.getPolyY(i + 1) - luceneXYPolygon.getPolyY(i) * luceneXYPolygon
                .getPolyX(i + 1);
        }
        return Math.abs(windingSum / 2);
    }

    public static ShapeRelation randomShapeRelation() {
        return RandomPicks.randomFrom(
            Randomness.get(),
            List.of(ShapeRelation.CONTAINS, ShapeRelation.WITHIN, ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS)
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

    private static Geometry randomGeometry(int size, boolean hasZCoords) {
        return RandomPicks.randomFrom(
            Randomness.get(),
            List.of(randomLine(size, hasZCoords), randomMultiPoint(size, hasZCoords), randomPoint(hasZCoords))
        );
    }

    public static GeometryCollection<Geometry> randomGeometryCollection(int maximum, boolean hasZCoords) {
        // Test at leaset 2 items in collection
        int size = OpenSearchTestCase.randomIntBetween(2, maximum);
        List<Geometry> shapes = new ArrayList<>();
        for (int count = 0; count < size; count++) {
            shapes.add(randomGeometry(size, hasZCoords));
        }
        return new GeometryCollection<>(shapes);
    }

    public static Rectangle randomRectangle() {
        double[] x = randomPointsForRectangle();
        double[] y = randomPointsForRectangle();
        return new Rectangle(x[0], x[1], y[1], y[0]);
    }

    private static double[] randomPointsForRectangle() {
        double first = randomDouble();
        double second = randomDouble();
        while (first == second) {
            second = randomDouble();
        }
        double[] vertex = new double[2];
        vertex[0] = Math.min(first, second);
        vertex[1] = Math.max(first, second);
        return vertex;
    }

    public static Geometry randomGeometryWithXYCoordinates() {
        boolean hasZCoords = false;
        int size = randomIntBetween(MIN_VERTEX, MIN_VERTEX + MAX_TOTAL_VERTEX);
        int attempt = 0;
        while (attempt++ < MAX_ATTEMPT_TO_RETURN_GEOMETRY) {
            try {
                return RandomPicks.randomFrom(
                    Randomness.get(),
                    List.of(
                        randomLine(size, hasZCoords),
                        randomMultiPoint(size, hasZCoords),
                        randomPoint(hasZCoords),
                        randomPolygon(),
                        randomMultiPolygon(),
                        randomMultiPoint(size, hasZCoords),
                        randomRectangle()
                    )
                );
            } catch (IOException | ParseException e) {
                // if for some reason we failed to generate geometry, try till we reach max attempt
            }
        }
        throw new RuntimeException("failed to generate random geometry");
    }

    public static List<XYPoint> getRandomXYPoints(int size, boolean hasZCoords) {
        List<XYPoint> xyPoints = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            Point point = randomPoint(hasZCoords);
            XYPoint xyPoint = new XYPoint((float) point.getX(), (float) point.getY());
            xyPoints.add(xyPoint);
        }
        return xyPoints;
    }

}
