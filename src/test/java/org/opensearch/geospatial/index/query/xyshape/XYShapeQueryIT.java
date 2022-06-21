/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xyshape;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.index.query.AbstractGeometryQueryBuilder.DEFAULT_SHAPE_FIELD_NAME;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.search.SearchHit;

public class XYShapeQueryIT extends GeospatialRestTestCase {

    private static final String INDEXED_SHAPE_FIELD = "indexed_shape";
    private static final String SHAPE_INDEX_FIELD = "index";
    private static final String SHAPE_ID_FIELD = "id";
    private static final String SHAPE_INDEX_PATH_FIELD = "path";
    private String indexName;
    private String xyShapeFieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        xyShapeFieldName = randomLowerCaseString();
    }

    public void testNullShape() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        String body = buildContentAsString(builder -> builder.field(xyShapeFieldName, (String) null));
        String docID = indexDocument(indexName, body);

        final Map<String, Object> document = getDocument(docID, indexName);
        assertTrue("failed to index document with type", document.containsKey(xyShapeFieldName));
        assertNull("failed to accept null value", document.get(xyShapeFieldName));

        deleteIndex(indexName);

    }

    public void testIndexPointsFilterRectangleWithDefaultRelation() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        // Will index two points and search with envelope that will intersect only one point
        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-30 -30)");
        final String firstDocumentID = indexDocument(indexName, firstDocument);

        Point point = new Point(-45, -50);
        final String secondDocument = buildDocumentWithGeoJSON(xyShapeFieldName, point);
        indexDocument(indexName, secondDocument);

        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(rectangle, builder, EMPTY_PARAMS);
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);

        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsFilterRectangleWithIntersectsRelation() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-30 -30)");
        final String firstDocumentID = indexDocument(indexName, firstDocument);

        final String secondDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-45 -50)");
        indexDocument(indexName, secondDocument);

        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(rectangle, builder, EMPTY_PARAMS);
            builder.field("relation", ShapeRelation.INTERSECTS.getRelationName());
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);

        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsCircle() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-131 -30)");
        indexDocument(indexName, firstDocument);

        final String secondDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-45 -50)");
        final String secondDocumentID = indexDocument(indexName, secondDocument);

        Circle circle = new Circle(-30, -30, 100);
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(circle, builder, EMPTY_PARAMS);
            builder.field("relation", ShapeRelation.INTERSECTS.getRelationName());
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);

        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(secondDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsPolygon() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-30 -30)");
        final String firstDocumentID = indexDocument(indexName, firstDocument);

        final String secondDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-45 -50)");
        indexDocument(indexName, secondDocument);

        double[] x = new double[] { -35, -35, -25, -25, -35 };
        double[] y = new double[] { -35, -25, -25, -35, -35 };
        LinearRing ring = new LinearRing(x, y);
        Polygon polygon = new Polygon(ring);
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(polygon, builder, EMPTY_PARAMS);
            builder.field("relation", ShapeRelation.INTERSECTS.getRelationName());
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);

        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsMultiPolygon() throws Exception {

        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-30 -30)");
        final String firstDocumentID = indexDocument(indexName, firstDocument);

        final String secondDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-40 -40)");
        indexDocument(indexName, secondDocument);

        final String thirdDocument = buildDocumentWithGeoJSON(xyShapeFieldName, new Point(-50, -50));
        final String thirdDocumentId = indexDocument(indexName, thirdDocument);

        LinearRing ring1 = new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 });
        Polygon polygon1 = new Polygon(ring1);

        LinearRing ring2 = new LinearRing(new double[] { -55, -55, -45, -45, -55 }, new double[] { -55, -45, -45, -55, -55 });
        Polygon polygon2 = new Polygon(ring2);

        MultiPolygon multiPolygon = new MultiPolygon(List.of(polygon1, polygon2));
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(multiPolygon, builder, EMPTY_PARAMS);
            builder.field("relation", ShapeRelation.INTERSECTS.getRelationName());
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);
        List<String> expectedDocIDs = List.of(firstDocumentID, thirdDocumentId);
        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, expectedDocIDs.size());
        List<String> actualDocIDS = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            actualDocIDS.add(hit.getId());
        }
        MatcherAssert.assertThat(expectedDocIDs, Matchers.containsInAnyOrder(actualDocIDS.toArray()));
    }

    public void testIndexPointsIndexedRectangleMatches() throws Exception {

        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        // Will index two points and search with envelope that will intersect only one point
        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-30 -30)");
        indexDocument(indexName, firstDocument);

        Point point = new Point(-45, -50);
        final String secondDocument = buildDocumentWithGeoJSON(xyShapeFieldName, point);
        final String secondDocumentID = indexDocument(indexName, secondDocument);

        // create an index to insert shape
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));

        final String indexedRefDoc1 = buildDocumentWithWKT(indexedShapePath, "BBOX(-50, -40, -45, -55)");
        final String shape = indexDocument(indexedShapeIndex, indexedRefDoc1);

        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.startObject(INDEXED_SHAPE_FIELD);
            builder.field(SHAPE_INDEX_FIELD, indexedShapeIndex);
            builder.field(SHAPE_ID_FIELD, shape);
            builder.field(SHAPE_INDEX_PATH_FIELD, indexedShapePath);
            builder.endObject();
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);

        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(secondDocumentID));

        deleteIndex(indexName);
        deleteIndex(indexedShapeIndex);
    }

    public void testIndexPointsIndexedRectangleNoMatch() throws Exception {

        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        // Will index two points and search with envelope that will intersect only one point
        final String firstDocument = buildDocumentWithWKT(xyShapeFieldName, "POINT(-30 -30)");
        indexDocument(indexName, firstDocument);

        Point point = new Point(-45, -50);
        final String secondDocument = buildDocumentWithGeoJSON(xyShapeFieldName, point);
        indexDocument(indexName, secondDocument);

        // create an index to insert shape
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));

        final String indexedRefDoc2 = buildDocumentWithWKT(indexedShapePath, "BBOX(-60, -50, -50, -60)");
        final String shape = indexDocument(indexedShapeIndex, indexedRefDoc2);

        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.startObject(INDEXED_SHAPE_FIELD);
            builder.field(SHAPE_INDEX_FIELD, indexedShapeIndex);
            builder.field(SHAPE_ID_FIELD, shape);
            builder.field(SHAPE_INDEX_PATH_FIELD, indexedShapePath);
            builder.endObject();
        }, XYShapeQueryBuilder.NAME, xyShapeFieldName);

        final SearchResponse searchResponse = searchIndex(indexName, searchEntity);

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 0);

        deleteIndex(indexName);
        deleteIndex(indexedShapeIndex);
    }

}
