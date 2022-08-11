/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xypoint;

import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYCircle;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYPolygon;
import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYRectangle;

import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

import lombok.AllArgsConstructor;

import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
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
import org.opensearch.geometry.ShapeType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

/**
 * Geometry Visitor to create a query to find all cartesian XYPoints
 * that comply ShapeRelation with all other XYShapes objects
 */
@AllArgsConstructor
public class XYPointQueryVisitor implements GeometryVisitor<Query, RuntimeException> {
    private final String fieldName;
    private final MappedFieldType fieldType;
    private final QueryShardContext context;

    /**
     * @param line  input geometry {@link Line}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     * throws QueryShardException
     */
    @Override
    public Query visit(Line line) {
        return geometryNotSupported(ShapeType.LINESTRING);
    }

    /**
     * @param ring  input geometry {@link LinearRing}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     * throws QueryShardException
     */
    @Override
    public Query visit(LinearRing ring) {
        return geometryNotSupported(ShapeType.LINEARRING);
    }

    /**
     * @param multiLine  input geometry {@link MultiLine}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     * throws QueryShardException
     */
    @Override
    public Query visit(MultiLine multiLine) {
        return geometryNotSupported(ShapeType.MULTILINESTRING);
    }

    /**
     * @param multiPoint  input geometry {@link MultiPoint}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     * throws QueryShardException
     */
    @Override
    public Query visit(MultiPoint multiPoint) {
        return geometryNotSupported(ShapeType.MULTIPOINT);
    }

    /**
     * @param point  input geometry {@link Point}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     * throws QueryShardException
     */
    @Override
    public Query visit(Point point) {
        return geometryNotSupported(ShapeType.POINT);
    }

    /**
     * @param circle  input geometry {@link Circle}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     * throws QueryShardException
     */
    @Override
    public Query visit(Circle circle) throws RuntimeException {
        Objects.requireNonNull(circle, "Circle cannot be null");
        XYCircle xyCircle = toXYCircle(circle);
        var query = XYPointField.newDistanceQuery(fieldName, xyCircle.getX(), xyCircle.getY(), xyCircle.getRadius());
        if (!fieldType.hasDocValues()) {
            return query;
        }

        var dvQuery = XYDocValuesField.newSlowDistanceQuery(fieldName, xyCircle.getX(), xyCircle.getY(), xyCircle.getRadius());
        return new IndexOrDocValuesQuery(query, dvQuery);
    }

    /**
     * @param rectangle  input geometry {@link Rectangle}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     */
    @Override
    public Query visit(Rectangle rectangle) {
        Objects.requireNonNull(rectangle, "Rectangle cannot be null");
        XYRectangle xyRectangle = toXYRectangle(rectangle);
        var query = XYPointField.newBoxQuery(fieldName, xyRectangle.minX, xyRectangle.maxX, xyRectangle.minY, xyRectangle.maxY);
        if (!fieldType.hasDocValues()) {
            return query;
        }

        var dvQuery = XYDocValuesField.newSlowBoxQuery(fieldName, xyRectangle.minX, xyRectangle.maxX, xyRectangle.minY, xyRectangle.maxY);
        return new IndexOrDocValuesQuery(query, dvQuery);
    }

    /**
     * @param multiPolygon  input geometry {@link MultiPolygon}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     */
    @Override
    public Query visit(MultiPolygon multiPolygon) {
        Objects.requireNonNull(multiPolygon, "Multi Polygon cannot be null");
        return visitCollection(multiPolygon);
    }

    /**
     * @param polygon  input geometry {@link Polygon}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     */
    @Override
    public Query visit(Polygon polygon) {
        Objects.requireNonNull(polygon, "Polygon cannot be null");
        return visitCollection(new GeometryCollection(Collections.singletonList(polygon)));
    }

    /**
     * @param collection  input geometry {@link GeometryCollection}
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     */
    @Override
    public Query visit(GeometryCollection<?> collection) {
        if (collection.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        var booleanQueryBuilder = new BooleanQuery.Builder();
        visit(booleanQueryBuilder, collection);
        return booleanQueryBuilder.build();
    }

    private void visit(BooleanQuery.Builder booleanQueryBuilder, GeometryCollection<?> collection) {
        var occur = BooleanClause.Occur.FILTER;
        for (Geometry shape : collection) {
            booleanQueryBuilder.add(shape.visit(this), occur);
        }
    }

    private Query visitCollection(GeometryCollection<Polygon> collection) {
        if (collection.isEmpty()) {
            return new MatchNoDocsQuery();
        }

        XYPolygon[] xyPolygons = new XYPolygon[collection.size()];
        for (int i = 0; i < collection.size(); i++) {
            xyPolygons[i] = toXYPolygon(collection.get(i));
        }

        var query = XYPointField.newPolygonQuery(fieldName, xyPolygons);
        if (!fieldType.hasDocValues()) {
            return query;
        }

        var dvQuery = XYDocValuesField.newSlowPolygonQuery(fieldName, xyPolygons);
        return new IndexOrDocValuesQuery(query, dvQuery);
    }

    private Query geometryNotSupported(ShapeType shapeType) {
        throw new QueryShardException(
            context,
            String.format(Locale.getDefault(), "Field [%s] found an unsupported shape [%s]", fieldName, shapeType.name())
        );
    }
}
