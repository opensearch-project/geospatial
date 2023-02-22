/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.geo.GeometryTestUtils.randomPoint;
import static org.opensearch.geospatial.GeospatialTestHelper.randomHexGridPrecision;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.h3.H3.geoToH3Address;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hamcrest.MatcherAssert;
import org.opensearch.client.ResponseException;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.geometry.Point;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.h3.H3;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

public class GeoHexAggregationIT extends GeospatialRestTestCase {

    private static final String FIELD = "field";
    private static final String FIELD_PRECISION = "precision";
    private static final String FIELD_SIZE = "size";
    private static int MAX_DOCUMENTS = 15;
    private static int MIN_DOCUMENTS = 2;
    private String indexName;
    private String geospatialFieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        geospatialFieldName = randomLowerCaseString();
    }

    public void testGeoHexGridBucket() throws Exception {
        // Step 1: Create an index
        createIndex(indexName, Settings.EMPTY, Map.of(geospatialFieldName, GeoPointFieldMapper.CONTENT_TYPE));

        // Generate metadata for Test data
        final var randomDocumentsForTesting = randomIntBetween(MIN_DOCUMENTS, MAX_DOCUMENTS);
        final var randomPrecision = randomHexGridPrecision();

        // Generate Test data
        final Map<Point, String> pointStringMap = generateRandomPointH3CellMap(randomDocumentsForTesting, randomPrecision);
        for (var point : pointStringMap.keySet()) {
            indexDocumentUsingWKT(indexName, geospatialFieldName, point.toString());
        }

        // do in-memory aggregation for comparison
        final Map<String, Long> expectedAggregationMap = pointStringMap.values()
            .stream()
            .collect(Collectors.groupingBy(identity(), Collectors.counting()));

        // build test aggregation search query
        var context = randomLowerCaseString();
        var content = buildSearchAggregationsBodyAsString(builder -> {
            builder.startObject(context)
                .startObject(GeoHexGridAggregationBuilder.NAME)
                .field(FIELD, geospatialFieldName)
                .field(FIELD_PRECISION, randomPrecision)
                .field(FIELD_SIZE, expectedAggregationMap.size())
                .endObject()
                .endObject();
        });

        // execute Aggregation
        final var searchResponse = searchIndex(indexName, content, true);
        // Assert Search succeeded
        assertSearchResponse(searchResponse);
        // Fetch Aggregation
        final var aggregation = searchResponse.getAggregations().asMap().get(context);
        assertNotNull(aggregation);

        // Assert Results
        assertTrue(aggregation instanceof MultiBucketsAggregation);
        final var multiBucketsAggregation = (MultiBucketsAggregation) aggregation;

        // Assert size before checking contents
        assertEquals(expectedAggregationMap.size(), multiBucketsAggregation.getBuckets().size());
        final Map<String, Long> actualAggregationMap = multiBucketsAggregation.getBuckets()
            .stream()
            .collect(toMap(MultiBucketsAggregation.Bucket::getKeyAsString, MultiBucketsAggregation.Bucket::getDocCount));

        // compare in-memory aggregation with cluster aggregation
        assertEquals(expectedAggregationMap, actualAggregationMap);

    }

    public void testSizeIsZero() throws Exception {

        // build test aggregation search query
        var context = randomLowerCaseString();
        var content = buildSearchAggregationsBodyAsString(builder -> {
            builder.startObject(context)
                .startObject(GeoHexGridAggregationBuilder.NAME)
                .field(FIELD, geospatialFieldName)
                .field(FIELD_PRECISION, randomHexGridPrecision())
                .field(FIELD_SIZE, 0)
                .endObject()
                .endObject();
        });

        // execute Aggregation
        ResponseException exception = expectThrows(ResponseException.class, () -> searchIndex(indexName, content, true));
        MatcherAssert.assertThat(exception.getMessage(), containsString("[size] must be greater than 0."));
    }

    public void testInvalidPrecision() throws Exception {

        // build test aggregation search query
        var invalidPrecision = H3.MAX_H3_RES + 1;
        var content = buildSearchAggregationsBodyAsString(builder -> {
            builder.startObject(randomLowerCaseString())
                .startObject(GeoHexGridAggregationBuilder.NAME)
                .field(FIELD, geospatialFieldName)
                .field(FIELD_PRECISION, invalidPrecision)
                .endObject()
                .endObject();
        });

        // execute Aggregation
        ResponseException exception = expectThrows(ResponseException.class, () -> searchIndex(indexName, content, true));
        MatcherAssert.assertThat(
            exception.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "Invalid precision of %d . Must be between %d and %d",
                    invalidPrecision,
                    H3.MIN_H3_RES,
                    H3.MAX_H3_RES
                )
            )
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(ClusterModule.getNamedXWriteables());
        final ContextParser<Object, Aggregation> hexGridParser = (p, c) -> ParsedGeoHexGrid.fromXContent(p, (String) c);
        namedXContents.add(
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(GeoHexGridAggregationBuilder.NAME), hexGridParser)
        );
        return new NamedXContentRegistry(namedXContents);
    }

    private Map<Point, String> generateRandomPointH3CellMap(int size, int randomPrecision) {
        return IntStream.range(0, size)
            .mapToObj(unUsed -> randomPoint())
            .collect(toMap(identity(), point -> geoToH3Address(point.getLat(), point.getLon(), randomPrecision)));
    }
}
