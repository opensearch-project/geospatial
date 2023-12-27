/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.MatcherAssert;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.geo.GeometryTestUtils;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.opensearch.geo.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.h3.H3;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

/**
 * This class is modified from https://github.com/opensearch-project/OpenSearch/blob/main/modules/geo/src/test/java/org/opensearch/geo/search/aggregations/bucket/geogrid/GeoGridAggregatorTestCase.java
 * to keep relevant test case required for GeoHex Grid Aggregation.
 */
public class GeoHexGridAggregatorTests extends AggregatorTestCase {

    private static final String GEO_POINT_FIELD_NAME = "location";
    private static final double TOLERANCE = 1E-5D;

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), GEO_POINT_FIELD_NAME, randomPrecision(), null, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, iw -> {
            // Intentionally not writing any docs
        });
    }

    public void testUnmapped() throws IOException {
        testCase(new MatchAllDocsQuery(), randomLowerCaseString(), randomPrecision(), null, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, iw -> { iw.addDocument(Collections.singleton(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, 10D, 10D))); });
    }

    public void testUnmappedMissing() throws IOException {
        GeoGridAggregationBuilder builder = createBuilder(randomLowerCaseString()).field(randomLowerCaseString())
            .missing("53.69437,6.475031");
        testCase(
            new MatchAllDocsQuery(),
            randomPrecision(),
            null,
            geoGrid -> assertEquals(1, geoGrid.getBuckets().size()),
            iw -> iw.addDocument(Collections.singleton(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, 10D, 10D))),
            builder
        );

    }

    public void testWithSeveralDocs() throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHex = new HashMap<>();
        testCase(new MatchAllDocsQuery(), GEO_POINT_FIELD_NAME, precision, null, geoHexGrid -> {
            assertEquals(expectedCountPerGeoHex.size(), geoHexGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHexGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHex.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            assertTrue(hasValue(geoHexGrid));
        }, iw -> {
            List<LatLonDocValuesField> points = new ArrayList<>();
            Set<String> distinctAddressPerDoc = new HashSet<>();
            for (int pointId = 0; pointId < numPoints; pointId++) {
                double[] latLng = randomLatLng();
                points.add(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, latLng[0], latLng[1]));
                String address = h3AddressAsString(latLng[1], latLng[0], precision);
                if (distinctAddressPerDoc.contains(address) == false) {
                    expectedCountPerGeoHex.put(address, expectedCountPerGeoHex.getOrDefault(address, 0) + 1);
                }
                distinctAddressPerDoc.add(address);
                if (usually()) {
                    iw.addDocument(points);
                    points.clear();
                    distinctAddressPerDoc.clear();
                }
            }
            if (points.size() != 0) {
                iw.addDocument(points);
            }
        });
    }

    public void testAsSubAgg() throws IOException {
        int precision = randomPrecision();
        Map<String, Map<String, Long>> expectedCountPerTPerGeoHex = new TreeMap<>();
        List<List<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            String t = randomAlphaOfLength(1);
            double[] latLng = randomLatLng();

            List<IndexableField> doc = new ArrayList<>();
            docs.add(doc);
            doc.add(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, latLng[0], latLng[1]));
            doc.add(new SortedSetDocValuesField("t", new BytesRef(t)));

            String address = h3AddressAsString(latLng[1], latLng[0], precision);
            Map<String, Long> expectedCountPerGeoHex = expectedCountPerTPerGeoHex.get(t);
            if (expectedCountPerGeoHex == null) {
                expectedCountPerGeoHex = new TreeMap<>();
                expectedCountPerTPerGeoHex.put(t, expectedCountPerGeoHex);
            }
            expectedCountPerGeoHex.put(address, expectedCountPerGeoHex.getOrDefault(address, 0L) + 1);
        }
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> iw.addDocuments(docs);
        String aggregation = randomLowerCaseString();
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("t").field("t")
            .size(expectedCountPerTPerGeoHex.size())
            .subAggregation(createBuilder(aggregation).field(GEO_POINT_FIELD_NAME).precision(precision));
        Consumer<StringTerms> verify = (terms) -> {
            Map<String, Map<String, Long>> actual = new TreeMap<>();
            for (StringTerms.Bucket tb : terms.getBuckets()) {
                GeoHexGrid gg = tb.getAggregations().get(aggregation);
                Map<String, Long> sub = new TreeMap<>();
                for (InternalGeoGridBucket ggb : gg.getBuckets()) {
                    sub.put(ggb.getKeyAsString(), ggb.getDocCount());
                }
                actual.put(tb.getKeyAsString(), sub);
            }
            MatcherAssert.assertThat(actual, equalTo(expectedCountPerTPerGeoHex));
        };
        testCase(aggregationBuilder, new MatchAllDocsQuery(), buildIndex, verify, keywordField("t"), geoPointField(GEO_POINT_FIELD_NAME));
    }

    public void testBounds() throws IOException {
        final int numDocs = randomIntBetween(64, 256);
        final GeoHexGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        // only consider bounding boxes that are at least TOLERANCE wide and have quantized coordinates
        GeoBoundingBox bbox = randomValueOtherThanMany(
            (b) -> Math.abs(GeoUtils.normalizeLon(b.right()) - GeoUtils.normalizeLon(b.left())) < TOLERANCE,
            GeoHexGridAggregatorTests::randomBBox
        );
        Function<Double, Double> encodeDecodeLat = (lat) -> GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        Function<Double, Double> encodeDecodeLon = (lon) -> GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        bbox.topLeft().reset(encodeDecodeLat.apply(bbox.top()), encodeDecodeLon.apply(bbox.left()));
        bbox.bottomRight().reset(encodeDecodeLat.apply(bbox.bottom()), encodeDecodeLon.apply(bbox.right()));

        int in = 0, out = 0;
        List<LatLonDocValuesField> docs = new ArrayList<>();
        while (in + out < numDocs) {
            if (bbox.left() > bbox.right()) {
                if (randomBoolean()) {
                    double lonWithin = randomBoolean()
                        ? randomDoubleBetween(bbox.left(), 180.0, true)
                        : randomDoubleBetween(-180.0, bbox.right(), true);
                    double latWithin = randomDoubleBetween(bbox.bottom(), bbox.top(), true);
                    in++;
                    docs.add(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, latWithin, lonWithin));
                } else {
                    double lonOutside = randomDoubleBetween(bbox.left(), bbox.right(), true);
                    double latOutside = randomDoubleBetween(bbox.top(), -90, false);
                    out++;
                    docs.add(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, latOutside, lonOutside));
                }
            } else {
                if (randomBoolean()) {
                    double lonWithin = randomDoubleBetween(bbox.left(), bbox.right(), true);
                    double latWithin = randomDoubleBetween(bbox.bottom(), bbox.top(), true);
                    in++;
                    docs.add(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, latWithin, lonWithin));
                } else {
                    double lonOutside = GeoUtils.normalizeLon(randomDoubleBetween(bbox.right(), 180.001, false));
                    double latOutside = GeoUtils.normalizeLat(randomDoubleBetween(bbox.top(), 90.001, false));
                    out++;
                    docs.add(new LatLonDocValuesField(GEO_POINT_FIELD_NAME, latOutside, lonOutside));
                }
            }

        }

        final long numDocsInBucket = in;
        final int precision = randomPrecision();

        testCase(new MatchAllDocsQuery(), GEO_POINT_FIELD_NAME, precision, bbox, geoGrid -> {
            assertTrue(hasValue(geoGrid));
            long docCount = 0;
            for (int i = 0; i < geoGrid.getBuckets().size(); i++) {
                docCount += geoGrid.getBuckets().get(i).getDocCount();
            }
            MatcherAssert.assertThat(docCount, equalTo(numDocsInBucket));
        }, iw -> {
            for (LatLonDocValuesField docField : docs) {
                iw.addDocument(Collections.singletonList(docField));
            }
        });
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }

    /**
     * Overriding the Search Plugins list with {@link GeospatialPlugin} so that the testcase will know that this plugin is
     * to be loaded during the tests.
     * @return List of {@link SearchPlugin}
     */
    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new GeospatialPlugin());
    }

    private double[] randomLatLng() {
        double lat = (180d * randomDouble()) - 90d;
        double lng = (360d * randomDouble()) - 180d;

        // Precision-adjust longitude/latitude to avoid wrong bucket placement
        // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
        // This does not affect geo hex because it also uses the same algorithm,
        // but it does affect other bucketing algos, thus we need to do the same steps here.
        lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
        lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

        return new double[] { lat, lng };
    }

    private void testCase(
        Query query,
        String field,
        int precision,
        GeoBoundingBox geoBoundingBox,
        Consumer<GeoHexGrid> verify,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex
    ) throws IOException {
        testCase(query, precision, geoBoundingBox, verify, buildIndex, createBuilder("_name").field(field));
    }

    private void testCase(
        Query query,
        int precision,
        GeoBoundingBox geoBoundingBox,
        Consumer<GeoHexGrid> verify,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        GeoGridAggregationBuilder aggregationBuilder
    ) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        aggregationBuilder.precision(precision);
        if (geoBoundingBox != null) {
            aggregationBuilder.setGeoBoundingBox(geoBoundingBox);
            MatcherAssert.assertThat(aggregationBuilder.geoBoundingBox(), equalTo(geoBoundingBox));
        }

        MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType(GEO_POINT_FIELD_NAME);

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((GeoHexGrid) aggregator.buildTopLevel());

        indexReader.close();
        directory.close();
    }

    private int randomPrecision() {
        return randomIntBetween(H3.MIN_H3_RES, H3.MAX_H3_RES);
    }

    private static boolean hasValue(GeoHexGrid agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    private static GeoBoundingBox randomBBox() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
    }

    private String h3AddressAsString(double lng, double lat, int precision) {
        return H3.geoToH3Address(lat, lng, precision);
    }

    private GeoHexGridAggregationBuilder createBuilder(String name) {
        return new GeoHexGridAggregationBuilder(name);
    }
}
