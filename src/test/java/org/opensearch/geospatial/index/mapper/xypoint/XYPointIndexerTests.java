/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import static org.mockito.Mockito.mock;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.getRandomXYPoints;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.index.IndexableField;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.test.OpenSearchTestCase;

public class XYPointIndexerTests extends OpenSearchTestCase {
    private XYPointIndexer indexer;
    private ParseContext parseContext;
    private final static String fieldName = "geometry";
    private final static Integer MIN_NUM_POINTS = 1;
    private final static Integer MAX_NUM_POINTS = 10;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexer = new XYPointIndexer(fieldName);
        parseContext = mock(ParseContext.class);
    }

    public void testIndexingNullGeometry() {
        expectThrows(NullPointerException.class, () -> indexer.prepareForIndexing(null));
    }

    public void testIndexingEmptyList() {
        expectThrows(IllegalArgumentException.class, () -> indexer.prepareForIndexing(Collections.emptyList()));
    }

    public void testPrepareIndexing() {
        var point = mock(org.opensearch.geospatial.index.mapper.xypoint.XYPoint.class);
        List<org.opensearch.geospatial.index.mapper.xypoint.XYPoint> points = List.of(point);
        assertNotNull("failed to convert xypoints from opensearch to lucene type", indexer.prepareForIndexing(points));
    }

    public void testIndexShape() {
        int numOfPoints = randomIntBetween(MIN_NUM_POINTS, MAX_NUM_POINTS);
        List<XYPoint> xyPoints = getRandomXYPoints(numOfPoints, randomBoolean());
        List<IndexableField> indexableFields = indexer.indexShape(parseContext, xyPoints);
        assertEquals("failed to index xypoints", numOfPoints, indexableFields.size());
    }
}
