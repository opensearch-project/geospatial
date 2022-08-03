/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query;

import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.index.query.AbstractGeometryQueryBuilder.DEFAULT_SHAPE_FIELD_NAME;

import java.io.IOException;
import java.util.Map;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.settings.Settings;
import org.opensearch.geometry.Geometry;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.geospatial.index.query.xyshape.XYShapeQueryBuilder;

public class GeospatialQueryTestHelperIT extends GeospatialRestTestCase {
    private static final String SHAPE_RELATION = "relation";
    private static final String INDEXED_SHAPE_FIELD = "indexed_shape";
    private static final String SHAPE_INDEX_FIELD = "index";
    private static final String SHAPE_ID_FIELD = "id";
    private static final String SHAPE_INDEX_PATH_FIELD = "path";

    public void queryTestUsingNullShape(String indexName, String fieldName, String contentType) throws Exception {
        createIndex(indexName, Settings.EMPTY, Map.of(fieldName, contentType));
        String body = buildContentAsString(builder -> builder.field(fieldName, (String) null));
        String docID = indexDocument(indexName, body);

        final Map<String, Object> document = getDocument(docID, indexName);
        assertTrue("failed to index document with type", document.containsKey(fieldName));
        assertNull("failed to accept null value", document.get(fieldName));

        deleteIndex(indexName);
    }

    public String indexDocumentUsingWKT(String indexName, String fieldName, String wktFormat) throws IOException {
        final String document = buildDocumentWithWKT(fieldName, wktFormat);
        return indexDocument(indexName, document);
    }

    public String indexDocumentUsingGeoJSON(String indexName, String fieldName, Geometry geometry) throws IOException {
        final String document = buildDocumentWithGeoJSON(fieldName, geometry);
        return indexDocument(indexName, document);
    }

    public SearchResponse searchUsingShapeRelation(String indexName, String fieldName, Geometry geometry, ShapeRelation shapeRelation)
        throws IOException {
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.field(DEFAULT_SHAPE_FIELD_NAME);
            GeoJson.toXContent(geometry, builder, EMPTY_PARAMS);
            builder.field(SHAPE_RELATION, shapeRelation.getRelationName());
        }, XYShapeQueryBuilder.NAME, fieldName);

        return searchIndex(indexName, searchEntity);
    }

    public void createIndexedShapeIndex() throws IOException {
        String indexedShapeIndex = randomLowerCaseString();
        String indexedShapePath = randomLowerCaseString();
        createIndex(indexedShapeIndex, Settings.EMPTY, Map.of(indexedShapePath, XYShapeFieldMapper.CONTENT_TYPE));
    }

    public SearchResponse searchUsingIndexedShapeIndex(
        String indexName,
        String indexedShapeIndex,
        String indexedShapePath,
        String docId,
        String fieldName
    ) throws IOException {
        String searchEntity = buildSearchBodyAsString(builder -> {
            builder.startObject(INDEXED_SHAPE_FIELD);
            builder.field(SHAPE_INDEX_FIELD, indexedShapeIndex);
            builder.field(SHAPE_ID_FIELD, docId);
            builder.field(SHAPE_INDEX_PATH_FIELD, indexedShapePath);
            builder.endObject();
        }, XYShapeQueryBuilder.NAME, fieldName);

        return searchIndex(indexName, searchEntity);
    }
}
