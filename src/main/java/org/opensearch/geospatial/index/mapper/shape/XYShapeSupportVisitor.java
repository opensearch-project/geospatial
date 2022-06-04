/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.shape;

import static org.opensearch.geometry.ShapeType.CIRCLE;
import static org.opensearch.geometry.ShapeType.LINEARRING;

import java.util.Locale;

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

// Visitor to check whether shape is supported for indexing or not
public final class XYShapeSupportVisitor implements GeometryVisitor<Geometry, RuntimeException> {
    @Override
    public Geometry visit(Circle circle) {
        throw new UnsupportedOperationException(String.format(Locale.getDefault(), "%s is not supported", CIRCLE));
    }

    @Override
    public Geometry visit(GeometryCollection<?> collection) {
        return collection;
    }

    @Override
    public Geometry visit(Line line) {
        return line;
    }

    @Override
    public Geometry visit(LinearRing ring) {
        throw new UnsupportedOperationException(String.format(Locale.getDefault(), "cannot index %s [ %s ] directly", LINEARRING, ring));
    }

    @Override
    public Geometry visit(MultiLine multiLine) {
        return multiLine;
    }

    @Override
    public Geometry visit(MultiPoint multiPoint) {
        return multiPoint;
    }

    @Override
    public Geometry visit(MultiPolygon multiPolygon) {
        return multiPolygon;
    }

    @Override
    public Geometry visit(Point point) {
        return point;
    }

    @Override
    public Geometry visit(Polygon polygon) {
        return polygon;
    }

    @Override
    public Geometry visit(Rectangle rectangle) {
        return rectangle;
    }
}
