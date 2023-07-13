/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.ShapeType;
import org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.AbstractQueryTestCase;

/**
 * The test cases are copied from GeoShapeQueryBuilderTests and updated to reflect
 * XYShapeQueryBuilder.
 */
public class XYShapeQueryBuilderTests extends AbstractQueryTestCase<XYShapeQueryBuilder> {

    private static final String XY_SHAPE_FIELD_NAME = "mapped_xy_shape";
    private static final String MAPPING_FIELD_TYPE_KEY = "type";
    private static final String DOC_TYPE = "_doc";

    private static String indexedShapeId;
    private static String indexedShapePath;
    private static String indexedShapeIndex;
    private static String indexedShapeRouting;
    private static Geometry indexedShapeToReturn;

    @Override
    protected XYShapeQueryBuilder doCreateTestQueryBuilder() {
        return createQueryBuilderFromQueryShape();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(GeospatialPlugin.class);
        return plugins;
    }

    @Override
    protected void doAssertLuceneQuery(XYShapeQueryBuilder xyShapeQueryBuilder, Query query, QueryShardContext queryShardContext) {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve AbstractGeometryQueryBuilder.doToQuery() method to make it
        // easier to test here
        MatcherAssert.assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            DOC_TYPE,
            new CompressedXContent(
                Strings.toString(
                    PutMappingRequest.simpleMapping(
                        XY_SHAPE_FIELD_NAME,
                        String.format(Locale.ROOT, "%s=%s", MAPPING_FIELD_TYPE_KEY, XYShapeQueryBuilder.NAME)
                    )
                )
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        MatcherAssert.assertThat(indexedShapeToReturn, notNullValue());
        MatcherAssert.assertThat(indexedShapeId, notNullValue());
        MatcherAssert.assertThat(getRequest.id(), equalTo(indexedShapeId));
        MatcherAssert.assertThat(getRequest.routing(), equalTo(indexedShapeRouting));
        String expectedShapeIndex = indexedShapeIndex == null ? XYShapeQueryBuilder.DEFAULT_SHAPE_INDEX_NAME : indexedShapeIndex;
        MatcherAssert.assertThat(getRequest.index(), equalTo(expectedShapeIndex));
        String expectedShapePath = indexedShapePath == null ? XYShapeQueryBuilder.DEFAULT_SHAPE_FIELD_NAME : indexedShapePath;

        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.field(expectedShapePath, (contentBuilder, params) -> GeoJson.toXContent(indexedShapeToReturn, contentBuilder, params));
            builder.field(randomAlphaOfLengthBetween(10, 20), randomLowerCaseString());
            builder.endObject();
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new OpenSearchException(ex);
        }
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeId, 0, 1, 0, true, new BytesArray(json), null, null));
    }

    @After
    public void clearShapeFields() {
        indexedShapeToReturn = null;
        indexedShapeId = null;
        indexedShapePath = null;
        indexedShapeIndex = null;
        indexedShapeRouting = null;
    }

    public void testNoFieldName() {
        Geometry shape = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new XYShapeQueryBuilder(null, shape));
        assertEquals("fieldName is required", e.getMessage());
    }

    public void testNoShape() {
        expectThrows(IllegalArgumentException.class, () -> new XYShapeQueryBuilder(XY_SHAPE_FIELD_NAME, (Geometry) null));
    }

    public void testNoRelation() {
        Geometry shape = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        XYShapeQueryBuilder builder = new XYShapeQueryBuilder(XYShapeQueryBuilder.NAME, shape);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.relation(null));
        assertEquals("No Shape Relation defined", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"xy_shape\" : {\n"
            + "    \"location\" : {\n"
            + "      \"shape\" : {\n"
            + "        \"type\" : \"Envelope\",\n"
            + "        \"coordinates\" : [ [ 13.0, 53.0 ], [ 14.0, 52.0 ] ]\n"
            + "      },\n"
            + "      \"relation\" : \"intersects\"\n"
            + "    },\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "}";
        XYShapeQueryBuilder parsed = (XYShapeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.0001);
    }

    @Override
    public void testMustRewrite() {
        XYShapeQueryBuilder query = createQueryBuilderFromIndexedShape();
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> query.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = rewriteAndFetch(query, createShardContext());
        XYShapeQueryBuilder geoShapeQueryBuilder = new XYShapeQueryBuilder(XY_SHAPE_FIELD_NAME, indexedShapeToReturn);
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testMultipleRewrite() {
        XYShapeQueryBuilder shape = createQueryBuilderFromIndexedShape();
        QueryBuilder builder = new BoolQueryBuilder().should(shape).should(shape);
        builder = rewriteAndFetch(builder, createShardContext());
        XYShapeQueryBuilder expectedShape = new XYShapeQueryBuilder(XY_SHAPE_FIELD_NAME, indexedShapeToReturn);
        expectedShape.relation(shape.relation());
        QueryBuilder expected = new BoolQueryBuilder().should(expectedShape).should(expectedShape);
        assertEquals(expected, builder);
    }

    public void testIgnoreUnmapped() throws IOException {
        Geometry shape = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        final XYShapeQueryBuilder queryBuilder = new XYShapeQueryBuilder("unmapped", shape);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        MatcherAssert.assertThat(query, notNullValue());
        MatcherAssert.assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testIgnoreUnmappedWithFailingQB() {
        Geometry shape = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        final XYShapeQueryBuilder failingQueryBuilder = new XYShapeQueryBuilder("unmapped", shape);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        MatcherAssert.assertThat(e.getMessage(), containsString("failed to find type for field [unmapped]"));
    }

    public void testWrongFieldType() {
        Geometry shape = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        final XYShapeQueryBuilder queryBuilder = new XYShapeQueryBuilder(TEXT_FIELD_NAME, shape);
        QueryShardException e = expectThrows(QueryShardException.class, () -> queryBuilder.toQuery(createShardContext()));
        MatcherAssert.assertThat(
            e.getMessage(),
            containsString("Field [mapped_string] is of unsupported type [text] for [xy_shape] query")
        );
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = createQueryBuilderFromIndexedShape();
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createShardContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createShardContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        MatcherAssert.assertThat(query, instanceOf(XYShapeQueryBuilder.class));
        return query;
    }

    private XYShapeQueryBuilder createQueryBuilderFromQueryShape() {
        clearShapeFields();
        Geometry geometry = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        XYShapeQueryBuilder builder = new XYShapeQueryBuilder(XY_SHAPE_FIELD_NAME, geometry);
        builder.ignoreUnmapped(randomBoolean());
        return setRelationBasedOnType(geometry.type(), builder);
    }

    private XYShapeQueryBuilder createQueryBuilderFromIndexedShape() {
        clearShapeFields();
        indexedShapeToReturn = ShapeObjectBuilder.randomGeometryWithXYCoordinates();
        indexedShapeId = randomLowerCaseString();
        XYShapeQueryBuilder builder = new XYShapeQueryBuilder(XY_SHAPE_FIELD_NAME, indexedShapeId);
        if (randomBoolean()) {
            indexedShapeIndex = randomLowerCaseString();
            builder.indexedShapeIndex(indexedShapeIndex);
        }
        if (randomBoolean()) {
            indexedShapePath = randomLowerCaseString();
            builder.indexedShapePath(indexedShapePath);
        }
        if (randomBoolean()) {
            indexedShapeRouting = randomLowerCaseString();
            builder.indexedShapeRouting(indexedShapeRouting);
        }
        builder.ignoreUnmapped(randomBoolean());
        return setRelationBasedOnType(indexedShapeToReturn.type(), builder);
    }

    private XYShapeQueryBuilder setRelationBasedOnType(ShapeType shapeType, XYShapeQueryBuilder builder) {
        if (shapeType == ShapeType.LINESTRING || shapeType == ShapeType.MULTILINESTRING) {
            return builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.CONTAINS));
        }
        return builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.WITHIN, ShapeRelation.CONTAINS));
    }
}
