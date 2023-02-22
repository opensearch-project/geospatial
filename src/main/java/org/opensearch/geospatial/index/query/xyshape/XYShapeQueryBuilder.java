/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.geo.parsers.ShapeParser;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geometry.Geometry;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeQueryable;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractGeometryQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

/**
 * Creates a new instance of {@link XYShapeQueryBuilder} to search XYShape based on input
 */
public class XYShapeQueryBuilder extends AbstractGeometryQueryBuilder<XYShapeQueryBuilder> {

    public static final String NAME = "xy_shape";

    /**
     * Creates a new instance of {@link XYShapeQueryBuilder} where query will be
     * performed on given fieldName based on given Geometry
     * @param fieldName xy_shape field to perform query
     * @param geometry Geometry used as input for query
     */
    public XYShapeQueryBuilder(String fieldName, Geometry geometry) {
        super(fieldName, geometry);
    }

    /**
     * Creates a new instance of {@link XYShapeQueryBuilder} where query will be
     * performed on given fieldName based on Document ID
     * @param fieldName xy_shape field to perform query
     * @param indexedShapeID Document ID corresponds to previously indexed xy_shape
     */
    public XYShapeQueryBuilder(String fieldName, String indexedShapeID) {
        super(fieldName, indexedShapeID);
    }

    public XYShapeQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private XYShapeQueryBuilder(String fieldName, Supplier<Geometry> supplier, String docID) {
        super(fieldName, supplier, docID);
    }

    @Override
    protected Query buildShapeQuery(QueryShardContext context, MappedFieldType fieldType) {
        if (fieldType instanceof XYShapeQueryable) {
            XYShapeQueryable query = (XYShapeQueryable) fieldType;
            return new ConstantScoreQuery(query.shapeQuery(shape, fieldName, relation, context));
        }
        throw new QueryShardException(
            context,
            String.format(Locale.ROOT, "Field [%s] is of unsupported type [%s] for [%s] query", fieldName, fieldType.typeName(), NAME)
        );
    }

    @Override
    protected void doShapeQueryXContent(XContentBuilder xContentBuilder, Params params) {
        // Only one strategy exists for XYShape, we don't have to implement this method
    }

    @Override
    protected XYShapeQueryBuilder newShapeQueryBuilder(String fieldName, Geometry geometry) {
        return new XYShapeQueryBuilder(fieldName, geometry);
    }

    @Override
    protected XYShapeQueryBuilder newShapeQueryBuilder(String fieldName, Supplier<Geometry> supplier, String docID) {
        return new XYShapeQueryBuilder(fieldName, supplier, docID);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * parse xy_shape query and extract query into {@link XYShapeQueryBuilder}
     * @param parser {@link XContentParser} Parser to parse xy_shape query input
     * @return XYShapeQueryBuilder instance contains xy_shape query value
     * @throws IOException if any of query param is invalid while parsing
     */
    public static XYShapeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        XYShapeQueryBuilder.ParsedXYShapeQueryParams params = (XYShapeQueryBuilder.ParsedXYShapeQueryParams) AbstractGeometryQueryBuilder
            .parsedParamsFromXContent(parser, new XYShapeQueryBuilder.ParsedXYShapeQueryParams());

        XYShapeQueryBuilder builder = createXYShapeQueryBuilder(params);

        if (Objects.nonNull(params.index)) { // if null, default index name: shapes will be used
            builder.indexedShapeIndex(params.index);
        }

        if (Objects.nonNull(params.shapePath)) { // if null, default filed name: shape will be used
            builder.indexedShapePath(params.shapePath);
        }

        if (Objects.nonNull(params.shapeRouting)) {
            builder.indexedShapeRouting(params.shapeRouting);
        }

        if (Objects.nonNull(params.relation)) { // if null, default relation: Intersects will be used
            builder.relation(params.relation);
        }

        if (Objects.nonNull(params.queryName)) {
            builder.queryName(params.queryName);
        }

        builder.boost(params.boost);
        builder.ignoreUnmapped(params.ignoreUnmapped);
        return builder;
    }

    private static XYShapeQueryBuilder createXYShapeQueryBuilder(ParsedXYShapeQueryParams params) {
        if (Objects.nonNull(params.shape)) {
            Geometry geometry = params.shape.buildGeometry();
            return new XYShapeQueryBuilder(params.fieldName, geometry);
        }
        return new XYShapeQueryBuilder(params.fieldName, params.id);
    }

    private static class ParsedXYShapeQueryParams extends AbstractGeometryQueryBuilder.ParsedGeometryQueryParams {
        @Override
        protected boolean parseXContentField(XContentParser parser) throws IOException {
            if (AbstractGeometryQueryBuilder.SHAPE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                this.shape = ShapeParser.parse(parser);
                return true;
            }
            return false;
        }
    }
}
