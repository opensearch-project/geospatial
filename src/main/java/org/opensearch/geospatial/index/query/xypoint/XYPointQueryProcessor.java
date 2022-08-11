/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xypoint;

import java.util.Locale;

import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

/**
 * Query Processor to convert given Geometry into Lucene query
 */
public class XYPointQueryProcessor {
    /**
     * Creates a {@link Query} that matches all indexed shapes to the provided {@link Geometry}  based on {@link ShapeRelation}
     *
     * @param shape  OpenSearch {@link Geometry} as an input
     * @param fieldName  field name that contains indexed points
     * @param relation  Relation to be used to get all points from given Geometry
     * @param context  QueryShardContext instance
     * @return {@link Query} instance from XYPointField.XYPointInGeometryQuery
     */
    public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        validateIsXYPointFieldType(fieldName, context);
        // XYPoint only support "intersects" spatial relation which returns points that are on the edge and inside the given geometry
        if (relation != ShapeRelation.INTERSECTS) {
            throw new QueryShardException(
                context,
                String.format(Locale.ROOT, "[%s] query relation not supported for Field [%s]", relation, fieldName)
            );
        }

        return getVectorQueryFromShape(shape, fieldName, context);
    }

    private void validateIsXYPointFieldType(String fieldName, QueryShardContext context) {
        var fieldType = context.fieldMapper(fieldName);
        if (fieldType instanceof XYPointFieldMapper.XYPointFieldType) {
            return;
        }

        throw new QueryShardException(
            context,
            String.format(
                Locale.ROOT,
                "Expected [%s] field type for Field [%s] but found [%s]",
                XYPointFieldMapper.CONTENT_TYPE,
                fieldName,
                fieldType.typeName()
            )
        );
    }

    protected Query getVectorQueryFromShape(Geometry queryShape, String fieldName, QueryShardContext context) {
        var xyPointQueryVisitor = new XYPointQueryVisitor(fieldName, context.fieldMapper(fieldName), context);
        return queryShape.visit(xyPointQueryVisitor);
    }
}
