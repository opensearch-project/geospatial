/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYCircle;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYLine;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYPoint;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYPolygon;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYRectangle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.geo.XYGeometry;
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
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

/**
 * Geometry Visitor to create a query to find all cartesian XYShapes
 * that comply ShapeRelation with all other XYShapes objects
 */
public class XYShapeQueryVisitor implements GeometryVisitor<List<XYGeometry>, RuntimeException> {

    private final String name;
    private final QueryShardContext context;

    public XYShapeQueryVisitor(String name, QueryShardContext context) {
        this.name = name;
        this.context = context;
    }

    @Override
    public List<XYGeometry> visit(Circle circle) throws RuntimeException {
        Objects.requireNonNull(circle, "Circle cannot be null");
        // XYShape don't support indexing Circle, but, can perform query to identify list of shapes
        // that interact with a provided circle
        return List.of(toXYCircle(circle));
    }

    @Override
    public List<XYGeometry> visit(GeometryCollection<?> geometryCollection) throws RuntimeException {
        Objects.requireNonNull(geometryCollection, "Geometry collection cannot be null");
        return visitCollection(geometryCollection);
    }

    @Override
    public List<XYGeometry> visit(Line line) throws RuntimeException {
        Objects.requireNonNull(line, "Line cannot be null");
        return List.of(toXYLine(line));
    }

    @Override
    public List<XYGeometry> visit(LinearRing linearRing) throws RuntimeException {
        throw new QueryShardException(
            this.context,
            String.format(Locale.ROOT, "Field [%s] found an unsupported shape LinearRing", this.name)
        );
    }

    @Override
    public List<XYGeometry> visit(MultiLine multiLine) throws RuntimeException {
        Objects.requireNonNull(multiLine, "Multi Line cannot be null");
        return visitCollection(multiLine);
    }

    @Override
    public List<XYGeometry> visit(MultiPoint multiPoint) throws RuntimeException {
        Objects.requireNonNull(multiPoint, "Multi Point cannot be null");
        return visitCollection(multiPoint);
    }

    @Override
    public List<XYGeometry> visit(MultiPolygon multiPolygon) throws RuntimeException {
        Objects.requireNonNull(multiPolygon, "Multi Polygon cannot be null");
        return visitCollection(multiPolygon);
    }

    @Override
    public List<XYGeometry> visit(Point point) throws RuntimeException {
        Objects.requireNonNull(point, "Point cannot be null");
        return List.of(toXYPoint(point));
    }

    @Override
    public List<XYGeometry> visit(Polygon polygon) throws RuntimeException {
        Objects.requireNonNull(polygon, "Polygon cannot be null");
        return List.of(toXYPolygon(polygon));
    }

    @Override
    public List<XYGeometry> visit(Rectangle rectangle) throws RuntimeException {
        Objects.requireNonNull(rectangle, "Rectangle cannot be null");
        return List.of(toXYRectangle(rectangle));
    }

    private List<XYGeometry> visitCollection(GeometryCollection<?> collection) {
        if (collection.isEmpty()) {
            return List.of();
        }
        List<XYGeometry> xyGeometryCollection = new ArrayList<>();
        for (Geometry geometry : collection) {
            xyGeometryCollection.addAll(geometry.visit(this));
        }
        return Collections.unmodifiableList(xyGeometryCollection);
    }
}
