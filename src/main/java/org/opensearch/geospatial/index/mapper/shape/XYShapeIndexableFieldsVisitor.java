/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.shape;

import static org.opensearch.geometry.ShapeType.CIRCLE;
import static org.opensearch.geometry.ShapeType.LINEARRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
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
import org.opensearch.geospatial.index.common.shape.ShapeConverter;

// Visitor to build only supported Shapes into Lucene indexable fields
public final class XYShapeIndexableFieldsVisitor implements GeometryVisitor<IndexableField[], RuntimeException> {
    private final String fieldName;

    public XYShapeIndexableFieldsVisitor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public IndexableField[] visit(Circle circle) {
        throw new IllegalArgumentException(
            String.format(Locale.getDefault(), "invalid shape type found [ %s ] while indexing shape", CIRCLE)
        );
    }

    @Override
    public IndexableField[] visit(GeometryCollection<?> collection) {
        Objects.requireNonNull(collection, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.GEOMETRYCOLLECTION));
        return visitCollection(collection);
    }

    @Override
    public IndexableField[] visit(Line line) {
        XYLine cartesianLine = ShapeConverter.toXYLine(line);
        return XYShape.createIndexableFields(fieldName, cartesianLine);
    }

    @Override
    public IndexableField[] visit(LinearRing ring) {
        throw new IllegalArgumentException(
            String.format(Locale.getDefault(), "invalid shape type found [ %s ] while indexing shape", LINEARRING)
        );
    }

    @Override
    public IndexableField[] visit(MultiLine multiLine) {
        Objects.requireNonNull(multiLine, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.MULTILINESTRING));
        return visitCollection(multiLine);
    }

    @Override
    public IndexableField[] visit(MultiPoint multiPoint) {
        Objects.requireNonNull(multiPoint, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.MULTIPOINT));
        return visitCollection(multiPoint);
    }

    @Override
    public IndexableField[] visit(MultiPolygon multiPolygon) {
        Objects.requireNonNull(multiPolygon, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.MULTIPOLYGON));
        return visitCollection(multiPolygon);
    }

    @Override
    public IndexableField[] visit(Point point) {
        Objects.requireNonNull(point, String.format(Locale.getDefault(), "%s cannot be null", ShapeType.POINT));
        float x = Double.valueOf(point.getX()).floatValue();
        float y = Double.valueOf(point.getY()).floatValue();
        return XYShape.createIndexableFields(fieldName, x, y);
    }

    @Override
    public IndexableField[] visit(Polygon polygon) {
        XYPolygon luceneXYPolygon = ShapeConverter.toXYPolygon(polygon);
        return createIndexableFields(luceneXYPolygon);
    }

    @Override
    public IndexableField[] visit(Rectangle rectangle) {
        XYPolygon luceneXYPolygon = ShapeConverter.toXYPolygon(rectangle);
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
