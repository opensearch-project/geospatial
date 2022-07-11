/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.query.QueryShardContext;

/**
 * Interface to build {@link Query} based on given {@link Geometry}
 */
public interface XYShapeQueryable {
    /**
     * Finds all previously indexed shapes that comply the given {@link ShapeRelation} with
     * the specified {@link Geometry}.
     * @param geometry query parameter to search indexed shapes
     * @param fieldName field name that contains indexed shapes
     * @param relation relation between search shape and indexed shape
     * @param context instance of {@link QueryShardContext}
     * @return Lucene {@link Query} to find indexed shapes based on given geometry
     */
    Query shapeQuery(Geometry geometry, String fieldName, ShapeRelation relation, QueryShardContext context);
}
