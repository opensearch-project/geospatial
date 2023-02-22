/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.geospatial.GeospatialTestHelper.randomHexGridPrecision;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.hamcrest.MatcherAssert;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.opensearch.geospatial.h3.H3;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;
import org.opensearch.test.InternalMultiBucketAggregationTestCase;

/**
 * This class is modified from https://github.com/opensearch-project/OpenSearch/blob/main/modules/geo/src/test/java/org/opensearch/geo/search/aggregations/bucket/geogrid/GeoGridTestCase.java
 *  to keep relevant test case required for GeoHex Grid.
 */
public class GeoHexGridTests extends InternalMultiBucketAggregationTestCase<GeoHexGrid> {

    private static final double LATITUDE_MIN = -90.0;
    private static final double LATITUDE_MAX = 90.0;
    private static final double LONGITUDE_MIN = -180.0;
    private static final double LONGITUDE_MAX = 180.0;
    private static final int MIN_BUCKET_SIZE = 1;
    private static final int MAX_BUCKET_SIZE = 3;

    public void testCreateFromBuckets() {
        InternalGeoGrid original = createTestInstance();
        MatcherAssert.assertThat(original, equalTo(original.create(original.getBuckets())));
    }

    @Override
    protected int minNumberOfBuckets() {
        return MIN_BUCKET_SIZE;
    }

    @Override
    protected int maxNumberOfBuckets() {
        return MAX_BUCKET_SIZE;
    }

    /**
     * Overriding the method so that tests can get the aggregation specs for namedWriteable.
     *
     * @return GeoSpatialPlugin
     */
    @Override
    protected SearchPlugin registerPlugin() {
        return new GeospatialPlugin();
    }

    /**
     * Overriding with the {@link ParsedGeoHexGrid} so that it can be parsed. We need to do this as {@link GeospatialPlugin}
     * is registering this Aggregation.
     *
     * @return a List of {@link NamedXContentRegistry.Entry}
     */
    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(getDefaultNamedXContents());
        final ContextParser<Object, Aggregation> hexGridParser = (p, c) -> ParsedGeoHexGrid.fromXContent(p, (String) c);
        namedXContents.add(
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(GeoHexGridAggregationBuilder.NAME), hexGridParser)
        );
        return namedXContents;
    }

    @Override
    protected GeoHexGrid createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        final int precision = randomHexGridPrecision();
        int size = randomNumberOfBuckets();
        List<InternalGeoGridBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            double latitude = randomDoubleBetween(LATITUDE_MIN, LATITUDE_MAX, false);
            double longitude = randomDoubleBetween(LONGITUDE_MIN, LONGITUDE_MAX, false);

            long addressAsLong = longEncode(longitude, latitude, precision);
            buckets.add(createInternalGeoGridBucket(addressAsLong, randomInt(IndexWriter.MAX_DOCS), aggregations));
        }
        return createInternalGeoGrid(name, size, buckets, metadata);
    }

    @Override
    protected void assertReduced(GeoHexGrid reduced, List<GeoHexGrid> inputs) {
        Map<Long, List<GeoHexGridBucket>> map = new HashMap<>();
        for (GeoHexGrid input : inputs) {
            for (GeoGrid.Bucket bucketBase : input.getBuckets()) {
                GeoHexGridBucket bucket = (GeoHexGridBucket) bucketBase;
                List<GeoHexGridBucket> buckets = map.computeIfAbsent(bucket.hashAsLong(), k -> new ArrayList<>());
                buckets.add(bucket);
            }
        }
        List<GeoHexGridBucket> expectedBuckets = new ArrayList<>();
        for (Map.Entry<Long, List<GeoHexGridBucket>> entry : map.entrySet()) {
            long docCount = 0;
            for (GeoHexGridBucket bucket : entry.getValue()) {
                docCount += bucket.getDocCount();
            }
            expectedBuckets.add(createInternalGeoGridBucket(entry.getKey(), docCount, InternalAggregations.EMPTY));
        }
        expectedBuckets.sort((first, second) -> {
            int cmp = Long.compare(second.getDocCount(), first.getDocCount());
            if (cmp == 0) {
                return second.compareTo(first);
            }
            return cmp;
        });
        int requestedSize = inputs.get(0).getRequiredSize();
        expectedBuckets = expectedBuckets.subList(0, Math.min(requestedSize, expectedBuckets.size()));
        assertEquals(expectedBuckets.size(), reduced.getBuckets().size());
        for (int i = 0; i < reduced.getBuckets().size(); i++) {
            GeoGrid.Bucket expected = expectedBuckets.get(i);
            GeoGrid.Bucket actual = reduced.getBuckets().get(i);
            assertEquals(expected.getDocCount(), actual.getDocCount());
            assertEquals(expected.getKey(), actual.getKey());
        }
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedGeoHexGrid.class;
    }

    @Override
    protected GeoHexGrid mutateInstance(GeoHexGrid instance) {
        String name = instance.getName();
        int size = instance.getRequiredSize();
        List<InternalGeoGridBucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    createInternalGeoGridBucket(randomNonNegativeLong(), randomInt(IndexWriter.MAX_DOCS), InternalAggregations.EMPTY)
                );
                break;
            case 2:
                size = size + between(1, 10);
                break;
            case 3:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return createInternalGeoGrid(name, size, buckets, metadata);
    }

    private GeoHexGrid createInternalGeoGrid(String name, int size, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata) {
        return new GeoHexGrid(name, size, buckets, metadata);
    }

    private GeoHexGridBucket createInternalGeoGridBucket(Long key, long docCount, InternalAggregations aggregations) {
        return new GeoHexGridBucket(key, docCount, aggregations);
    }

    private long longEncode(double lng, double lat, int precision) {
        return H3.geoToH3(lng, lat, precision);
    }
}
