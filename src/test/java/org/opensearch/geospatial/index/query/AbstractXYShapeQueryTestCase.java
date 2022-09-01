/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.search.SearchHit;

public abstract class AbstractXYShapeQueryTestCase extends GeospatialRestTestCase {

    public abstract String getIndexName();

    public abstract String getFieldName();

    public abstract String getContentType();

    public void testNullShape() throws Exception {
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));
        String body = buildContentAsString(builder -> builder.field(getFieldName(), (String) null));
        String docID = indexDocument(getIndexName(), body);

        final Map<String, Object> document = getDocument(docID, getIndexName());
        assertTrue("failed to index document with type", document.containsKey(getFieldName()));
        assertNull("failed to accept null value", document.get(getFieldName()));

        deleteIndex(getIndexName());
    }

    public void testIndexPointsFilterRectangleWithIntersectsRelation() throws Exception {
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));
        final String firstDocumentID = indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-30 -30)");
        indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-45 -50)");

        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);
        final SearchResponse searchResponse = searchUsingShapeRelation(getIndexName(), getFieldName(), rectangle, ShapeRelation.INTERSECTS);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(getIndexName());
    }

    public void testIndexPointsIndexedRectangleMatches() throws Exception {
        String secondDocumentID = "";
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));
        // Will index two points and search with envelope that will intersect only one point
        indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-30 -30)");
        if (XYPointFieldMapper.CONTENT_TYPE.equals(getContentType())) {
            secondDocumentID = indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-45 -50)");
        } else {
            secondDocumentID = indexDocumentUsingGeoJSON(getIndexName(), getFieldName(), new Point(-45, -50));
        }

        // create an index to insert shape
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));

        final String shapeDocID = indexDocumentUsingWKT(indexedShapeIndex, indexedShapePath, "BBOX(-50, -40, -45, -55)");

        final SearchResponse searchResponse = searchUsingIndexedShapeIndex(
            getIndexName(),
            indexedShapeIndex,
            indexedShapePath,
            shapeDocID,
            getFieldName()
        );
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(secondDocumentID));

        deleteIndex(getIndexName());
        deleteIndex(indexedShapeIndex);
    }

    public void testIndexPointsCircle() throws Exception {
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));

        indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-131 -30)");
        final String secondDocumentID = indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-45 -50)");

        Circle circle = new Circle(-30, -30, 100);

        final SearchResponse searchResponse = searchUsingShapeRelation(getIndexName(), getFieldName(), circle, ShapeRelation.INTERSECTS);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(secondDocumentID));

        deleteIndex(getIndexName());
    }

    public void testIndexPointsPolygon() throws Exception {
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));

        final String firstDocumentID = indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-30 -30)");
        indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-45 -50)");

        double[] x = new double[] { -35, -35, -25, -25, -35 };
        double[] y = new double[] { -35, -25, -25, -35, -35 };
        LinearRing ring = new LinearRing(x, y);
        Polygon polygon = new Polygon(ring);

        final SearchResponse searchResponse = searchUsingShapeRelation(getIndexName(), getFieldName(), polygon, ShapeRelation.INTERSECTS);
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        MatcherAssert.assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(firstDocumentID));

        deleteIndex(getIndexName());
    }

    public void testIndexPointsMultiPolygon() throws Exception {
        String thirdDocumentId = "";
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));

        final String firstDocumentID = indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-30 -30)");
        indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-40 -40)");
        if (XYPointFieldMapper.CONTENT_TYPE.equals(getContentType())) {
            thirdDocumentId = indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-50 -50)");
        } else {
            thirdDocumentId = indexDocumentUsingGeoJSON(getIndexName(), getFieldName(), new Point(-50, -50));
        }

        LinearRing ring1 = new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 });
        Polygon polygon1 = new Polygon(ring1);

        LinearRing ring2 = new LinearRing(new double[] { -55, -55, -45, -45, -55 }, new double[] { -55, -45, -45, -55, -55 });
        Polygon polygon2 = new Polygon(ring2);

        MultiPolygon multiPolygon = new MultiPolygon(List.of(polygon1, polygon2));
        List<String> expectedDocIDs = List.of(firstDocumentID, thirdDocumentId);

        final SearchResponse searchResponse = searchUsingShapeRelation(
            getIndexName(),
            getFieldName(),
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

        deleteIndex(getIndexName());
    }

    public void testIndexPointsIndexedRectangleNoMatch() throws Exception {
        createIndex(getIndexName(), Settings.EMPTY, Map.of(getFieldName(), getContentType()));

        indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-30 -30)");
        if (XYPointFieldMapper.CONTENT_TYPE.equals(getContentType())) {
            indexDocumentUsingWKT(getIndexName(), getFieldName(), "POINT(-45 -50)");
        } else {
            indexDocumentUsingGeoJSON(getIndexName(), getFieldName(), new Point(-45, -50));
        }

        // create an index to insert shape
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));

        final String shapeDocID = indexDocumentUsingWKT(indexedShapeIndex, indexedShapePath, "BBOX(-60, -50, -50, -60)");

        final SearchResponse searchResponse = searchUsingIndexedShapeIndex(
            getIndexName(),
            indexedShapeIndex,
            indexedShapePath,
            shapeDocID,
            getFieldName()
        );
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 0);

        deleteIndex(getIndexName());
        deleteIndex(indexedShapeIndex);
    }
}
