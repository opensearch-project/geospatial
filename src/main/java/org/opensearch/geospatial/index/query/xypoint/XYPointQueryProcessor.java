/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xypoint;

import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
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
        // XYPoint only support "intersects"
        if (relation != ShapeRelation.INTERSECTS) {
            throw new QueryShardException(context, relation + " query relation not supported for Field [" + fieldName + "].");
        }

        return getVectorQueryFromShape(shape, fieldName, context);
    }

    private void validateIsXYPointFieldType(String fieldName, QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType instanceof XYPointFieldMapper.XYPointFieldType) {
            return;
        }
        throw new QueryShardException(
            context,
            "Expected " + XYPointFieldMapper.CONTENT_TYPE + " field type for Field [" + fieldName + "] but found " + fieldType.typeName()
        );
    }

    protected Query getVectorQueryFromShape(Geometry queryShape, String fieldName, QueryShardContext context) {
        XYPointQueryVisitor xyPointQueryVisitor = new XYPointQueryVisitor(context, context.fieldMapper(fieldName), fieldName);
        return queryShape.visit(xyPointQueryVisitor);
    }
}
