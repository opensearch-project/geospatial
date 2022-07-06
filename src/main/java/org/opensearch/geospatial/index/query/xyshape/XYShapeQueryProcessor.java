/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import java.util.List;
import java.util.Locale;

import lombok.NonNull;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

/**
 * Query Processor to convert given Geometry into Lucene query
 */
public class XYShapeQueryProcessor {

    /**
     * Creates a {@link Query} that matches all indexed shapes to the provided {@link Geometry}  based on {@link ShapeRelation}
     * @param geometry OpenSearch {@link Geometry} as an input
     * @param fieldName Lucene field of type {@link XYShape}
     * @param relation Relation to be used to get all shapes from given Geometry
     * @param visitor {@link GeometryVisitor} to convert geometry to List of XYGeometry
     * @param context QueryShardContext instance
     * @return {@link Query} instance from XYShape.newGeometryQuery
     */
    public Query shapeQuery(
        Geometry geometry,
        @NonNull String fieldName,
        @NonNull ShapeRelation relation,
        @NonNull GeometryVisitor<List<XYGeometry>, RuntimeException> visitor,
        @NonNull QueryShardContext context
    ) {
        // if no input is parsed from input, return no documents should be matched from index,
        // this is similar to geo_shape field type
        if (geometry == null || geometry.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        validateIsXYShapeFieldType(fieldName, context);
        return getQueryFromGeometry(geometry, fieldName, relation.getLuceneRelation(), visitor);
    }

    private Query getQueryFromGeometry(
        Geometry geometry,
        String fieldName,
        ShapeField.QueryRelation queryRelation,
        GeometryVisitor<List<XYGeometry>, RuntimeException> visitor
    ) {
        final List<XYGeometry> collections = geometry.visit(visitor);
        // if no valid geometries are found from input shape, return No documents matched query as fallback
        if (collections == null || collections.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        return XYShape.newGeometryQuery(fieldName, queryRelation, collections.toArray(new XYGeometry[0]));
    }

    private void validateIsXYShapeFieldType(String fieldName, QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType instanceof XYShapeFieldMapper.XYShapeFieldType) {
            return;
        }
        throw new QueryShardException(
            context,
            String.format(
                Locale.getDefault(),
                "Expected %s field type for Field [ %s ] but found %s",
                XYShapeFieldMapper.CONTENT_TYPE,
                fieldName,
                fieldType.typeName()
            )
        );
    }
}
