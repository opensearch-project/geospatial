/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.GeometryParser;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geospatial.index.query.xyshape.XYShapeQueryProcessor;
import org.opensearch.geospatial.index.query.xyshape.XYShapeQueryVisitor;
import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.opensearch.index.mapper.GeoShapeParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.query.QueryShardContext;

/**
 *  FieldMapper for indexing {@link org.apache.lucene.document.XYShape}s.
 */
public class XYShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry, Geometry> {

    public static final String CONTENT_TYPE = "xy_shape";
    private static final FieldType FIELD_TYPE = new FieldType();
    // Similar to geo_shape, this field is indexed by encoding it as triangular mesh
    // and index each traingle as 7 dimension point in BKD Tree
    static {
        FIELD_TYPE.setDimensions(7, 4, Integer.BYTES);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    private XYShapeFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> coerce,
        Explicit<Boolean> ignoreZValue,
        Explicit<ShapeBuilder.Orientation> orientation,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation, multiFields, copyTo);
    }

    @Override
    public XYShapeFieldType fieldType() {
        return (XYShapeFieldType) super.fieldType();
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper mergeWith, List conflicts) {
        // Cartesian plane don't have to support this feature
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void addStoredFields(ParseContext context, Geometry geometry) {
        // No stored fields will be added
    }

    @Override
    protected void addDocValuesFields(String name, Geometry geometry, List<IndexableField> fields, ParseContext context) {
        // doc values are not supported
    }

    @Override
    protected void addMultiFields(ParseContext context, Geometry geometry) {
        // No other fields will be added
    }

    /**
     * Builder class to create an instance of {@link XYShapeFieldMapper}
     */
    public static class XYShapeFieldMapperBuilder extends AbstractShapeGeometryFieldMapper.Builder<
        XYShapeFieldMapperBuilder,
        XYShapeFieldType> {

        public XYShapeFieldMapperBuilder(String fieldName) {
            super(fieldName, FIELD_TYPE);
            this.hasDocValues = false;
        }

        @Override
        public XYShapeFieldMapper build(BuilderContext context) {
            return new XYShapeFieldMapper(
                name,
                fieldType,
                buildShapeFieldType(context),
                ignoreMalformed(context),
                coerce(context),
                ignoreZValue(),
                orientation(),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }

        private XYShapeFieldType buildShapeFieldType(BuilderContext context) {
            XYShapeQueryProcessor processor = new XYShapeQueryProcessor();
            XYShapeFieldType fieldType = new XYShapeFieldType(
                buildFullName(context),
                indexed,
                this.fieldType.stored(),
                hasDocValues,
                meta,
                processor
            );
            GeometryParser geometryParser = new GeometryParser(
                orientation().value().getAsBoolean(),
                coerce().value(),
                ignoreZValue().value()
            );
            fieldType.setGeometryParser(new GeoShapeParser(geometryParser));
            GeometryVisitor<IndexableField[], RuntimeException> xyShapeIndexableVisitor = new XYShapeIndexableFieldsVisitor(
                fieldType.name()
            );
            GeometryVisitor<Geometry, RuntimeException> xyShapeSupportVisitor = new XYShapeSupportVisitor();
            fieldType.setGeometryIndexer(new XYShapeIndexer(xyShapeSupportVisitor, xyShapeIndexableVisitor));
            fieldType.setOrientation(orientation().value());
            return fieldType;
        }
    }

    public static class XYShapeFieldType extends AbstractShapeGeometryFieldType<Geometry, Geometry> implements XYShapeQueryable {

        private final XYShapeQueryProcessor queryProcessor;

        public XYShapeFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Map<String, String> meta,
            XYShapeQueryProcessor processor
        ) {
            super(name, indexed, stored, hasDocValues, false, meta);
            this.queryProcessor = Objects.requireNonNull(processor, "query processor cannot be null");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query shapeQuery(Geometry geometry, String fieldName, ShapeRelation relation, QueryShardContext context) {
            GeometryVisitor<List<XYGeometry>, RuntimeException> visitor = new XYShapeQueryVisitor(fieldName, context);
            return this.queryProcessor.shapeQuery(geometry, fieldName, relation, visitor, context);
        }

    }

}
