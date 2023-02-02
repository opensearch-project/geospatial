/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

import static org.opensearch.geometry.ShapeType.CIRCLE;
import static org.opensearch.geometry.ShapeType.LINEARRING;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexableField;
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
import org.opensearch.geospatial.index.common.xyshape.XYShapeConverter;

/**
 * Visitor to build only supported Shapes into Lucene indexable fields
 */
public final class XYShapeIndexableFieldsVisitor implements GeometryVisitor<IndexableField[], RuntimeException> {
    private final String fieldName;

    public XYShapeIndexableFieldsVisitor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public IndexableField[] visit(Circle circle) {
        throw new IllegalArgumentException(String.format(Locale.ROOT, "invalid shape type found [ %s ] while indexing shape", CIRCLE));
    }

    @Override
    public IndexableField[] visit(GeometryCollection<?> collection) {
        Objects.requireNonNull(collection, String.format(Locale.ROOT, "%s cannot be null", ShapeType.GEOMETRYCOLLECTION));
        return visitCollection(collection);
    }

    @Override
    public IndexableField[] visit(Line line) {
        XYLine cartesianLine = XYShapeConverter.toXYLine(line);
        return XYShape.createIndexableFields(fieldName, cartesianLine);
    }

    @Override
    public IndexableField[] visit(LinearRing ring) {
        throw new IllegalArgumentException(String.format(Locale.ROOT, "invalid shape type found [ %s ] while indexing shape", LINEARRING));
    }

    @Override
    public IndexableField[] visit(MultiLine multiLine) {
        Objects.requireNonNull(multiLine, String.format(Locale.ROOT, "%s cannot be null", ShapeType.MULTILINESTRING));
        return visitCollection(multiLine);
    }

    @Override
    public IndexableField[] visit(MultiPoint multiPoint) {
        Objects.requireNonNull(multiPoint, String.format(Locale.ROOT, "%s cannot be null", ShapeType.MULTIPOINT));
        return visitCollection(multiPoint);
    }

    @Override
    public IndexableField[] visit(MultiPolygon multiPolygon) {
        Objects.requireNonNull(multiPolygon, String.format(Locale.ROOT, "%s cannot be null", ShapeType.MULTIPOLYGON));
        return visitCollection(multiPolygon);
    }

    @Override
    public IndexableField[] visit(Point point) {
        Objects.requireNonNull(point, String.format(Locale.ROOT, "%s cannot be null", ShapeType.POINT));
        XYPoint xyPoint = toXYPoint(point);
        return XYShape.createIndexableFields(fieldName, xyPoint.getX(), xyPoint.getY());
    }

    @Override
    public IndexableField[] visit(Polygon polygon) {
        XYPolygon luceneXYPolygon = XYShapeConverter.toXYPolygon(polygon);
        return createIndexableFields(luceneXYPolygon);
    }

    @Override
    public IndexableField[] visit(Rectangle rectangle) {
        XYPolygon luceneXYPolygon = XYShapeConverter.toXYPolygon(rectangle);
        return createIndexableFields(luceneXYPolygon);
    }

    private IndexableField[] createIndexableFields(XYPolygon polygon) {
        return XYShape.createIndexableFields(fieldName, polygon);
    }

    private IndexableField[] visitCollection(GeometryCollection<?> collection) {
        List<IndexableField> fields = new ArrayList<>();
        collection.forEach(geometry -> fields.addAll(Arrays.asList(geometry.visit(this))));
        return fields.toArray(IndexableField[]::new);
    }
}
