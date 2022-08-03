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
import org.opensearch.geospatial.index.query.GeospatialQueryTestHelperIT;
import org.opensearch.search.SearchHit;

public class XYShapeQueryIT extends GeospatialRestTestCase {
    private String indexName;
    private String xyShapeFieldName;
    private GeospatialQueryTestHelperIT geospatialQueryTestHelperIT;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        xyShapeFieldName = randomLowerCaseString();
        geospatialQueryTestHelperIT = new GeospatialQueryTestHelperIT();
    }

    public void testNullShape() throws Exception {
        geospatialQueryTestHelperIT.queryTestUsingNullShape(indexName, xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE);

    }

    public void testIndexPointsFilterRectangleWithDefaultRelation() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        // Will index two points and search with envelope that will intersect only one point
        final String firstDocumentID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        geospatialQueryTestHelperIT.indexDocumentUsingGeoJSON(indexName, xyShapeFieldName, new Point(-45, -50));

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
        final String firstDocumentID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-45 -50)");

        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);
        final SearchResponse searchResponse = geospatialQueryTestHelperIT.searchUsingShapeRelation(
            indexName,
            xyShapeFieldName,
            rectangle,
            ShapeRelation.INTERSECTS
        );
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsCircle() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-131 -30)");
        final String secondDocumentID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-45 -50)");

        Circle circle = new Circle(-30, -30, 100);

        final SearchResponse searchResponse = geospatialQueryTestHelperIT.searchUsingShapeRelation(
            indexName,
            xyShapeFieldName,
            circle,
            ShapeRelation.INTERSECTS
        );
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(secondDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsPolygon() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        final String firstDocumentID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-45 -50)");

        double[] x = new double[] { -35, -35, -25, -25, -35 };
        double[] y = new double[] { -35, -25, -25, -35, -35 };
        LinearRing ring = new LinearRing(x, y);
        Polygon polygon = new Polygon(ring);

        final SearchResponse searchResponse = geospatialQueryTestHelperIT.searchUsingShapeRelation(
            indexName,
            xyShapeFieldName,
            polygon,
            ShapeRelation.INTERSECTS
        );
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(indexName);
    }

    public void testIndexPointsMultiPolygon() throws Exception {

        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));

        final String firstDocumentID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-40 -40)");
        final String thirdDocumentId = geospatialQueryTestHelperIT.indexDocumentUsingGeoJSON(
            indexName,
            xyShapeFieldName,
            new Point(-50, -50)
        );

        LinearRing ring1 = new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 });
        Polygon polygon1 = new Polygon(ring1);

        LinearRing ring2 = new LinearRing(new double[] { -55, -55, -45, -45, -55 }, new double[] { -55, -45, -45, -55, -55 });
        Polygon polygon2 = new Polygon(ring2);

        MultiPolygon multiPolygon = new MultiPolygon(List.of(polygon1, polygon2));
        List<String> expectedDocIDs = List.of(firstDocumentID, thirdDocumentId);
        final SearchResponse searchResponse = geospatialQueryTestHelperIT.searchUsingShapeRelation(
            indexName,
            xyShapeFieldName,
            multiPolygon,
            ShapeRelation.INTERSECTS
        );
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
        geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        final String secondDocumentID = geospatialQueryTestHelperIT.indexDocumentUsingGeoJSON(
            indexName,
            xyShapeFieldName,
            new Point(-45, -50)
        );

        // create an index to insert shape
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));

        final String shapeDocID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(
            indexedShapeIndex,
            indexedShapePath,
            "BBOX(-50, -40, -45, -55)"
        );

        final SearchResponse searchResponse = geospatialQueryTestHelperIT.searchUsingIndexedShapeIndex(
            indexName,
            indexedShapeIndex,
            indexedShapePath,
            shapeDocID,
            xyShapeFieldName
        );
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(secondDocumentID));

        deleteIndex(indexName);
        deleteIndex(indexedShapeIndex);
    }

    public void testIndexPointsIndexedRectangleNoMatch() throws Exception {

        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        // Will index two points and search with envelope that will intersect only one point
        geospatialQueryTestHelperIT.indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        geospatialQueryTestHelperIT.indexDocumentUsingGeoJSON(indexName, xyShapeFieldName, new Point(-45, -50));

        // create an index to insert shape
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));
        final String shapeDocID = geospatialQueryTestHelperIT.indexDocumentUsingWKT(
            indexedShapeIndex,
            indexedShapePath,
            "BBOX(-60, -50, -50, -60)"
        );

        final SearchResponse searchResponse = geospatialQueryTestHelperIT.searchUsingIndexedShapeIndex(
            indexName,
            indexedShapeIndex,
            indexedShapePath,
            shapeDocID,
            xyShapeFieldName
        );

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 0);

        deleteIndex(indexName);
        deleteIndex(indexedShapeIndex);
    }

}
