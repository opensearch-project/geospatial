/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import static org.mockito.Mockito.mock;
import static org.opensearch.geospatial.index.common.xyshape.ShapeObjectBuilder.getRandomXYPoints;

import java.util.ArrayList;
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
        ParsedXYPoint parsedXYPoint = mock(ParsedXYPoint.class);
        ArrayList<ParsedXYPoint> points = new ArrayList<>();
        points.add(parsedXYPoint);
        assertNotNull(indexer.prepareForIndexing(points));
    }

    public void testIndexShape() {
        int numOfPoints = 1;
        List<XYPoint> xyPoints = getRandomXYPoints(numOfPoints, randomBoolean());
        List<IndexableField> indexableFields = indexer.indexShape(parseContext, xyPoints);
        assertNotNull(indexableFields.get(0));
    }

    public void testIndexShapeMultiPoints() {
        int numOfPoints = 3;
        List<XYPoint> xyPoints = getRandomXYPoints(numOfPoints, randomBoolean());
        List<IndexableField> indexableFields = indexer.indexShape(parseContext, xyPoints);
        assertNotNull(indexableFields.get(0));
        assertNotNull(indexableFields.get(1));
        assertNotNull(indexableFields.get(2));
    }
}
