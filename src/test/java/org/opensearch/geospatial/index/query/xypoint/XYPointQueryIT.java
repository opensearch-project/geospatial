/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query.xypoint;

import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.util.Map;

import org.opensearch.client.ResponseException;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.geospatial.index.query.AbstractXYShapeQueryTestCase;

public class XYPointQueryIT extends AbstractXYShapeQueryTestCase {
    private String indexName;
    private String xyPointFieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        xyPointFieldName = randomLowerCaseString();
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getFieldName() {
        return xyPointFieldName;
    }

    @Override
    public String getContentType() {
        return XYPointFieldMapper.CONTENT_TYPE;
    }

    public void testIndexPointsFilterRectangleWithUnsupportedRelation() throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(xyPointFieldName, XYPointFieldMapper.CONTENT_TYPE));

        final String firstDocument = buildDocumentWithWKT(xyPointFieldName, "POINT(-30 -30)");
        indexDocument(indexName, firstDocument);

        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> searchUsingShapeRelation(indexName, xyPointFieldName, rectangle, ShapeRelation.CONTAINS)
        );
        assertTrue(exception.getMessage().contains("[CONTAINS] query relation not supported"));

        deleteIndex(indexName);
    }
}
