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

import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.geospatial.index.query.AbstractXYShapeQueryTestCase;

public class XYShapeQueryIT extends AbstractXYShapeQueryTestCase {
    private String indexName;
    private String xyShapeFieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        xyShapeFieldName = randomLowerCaseString();
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getFieldName() {
        return xyShapeFieldName;
    }

    @Override
    public String getContentType() {
        return XYShapeFieldMapper.CONTENT_TYPE;
    }

    public void testIndexPointsFilterRectangleWithDefaultRelation() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyShapeFieldName, XYShapeFieldMapper.CONTENT_TYPE));
        // Will index two points and search with envelope that will intersect only one point
        final String firstDocumentID = indexDocumentUsingWKT(indexName, xyShapeFieldName, "POINT(-30 -30)");
        indexDocumentUsingGeoJSON(indexName, xyShapeFieldName, new Point(-45, -50));

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
}
