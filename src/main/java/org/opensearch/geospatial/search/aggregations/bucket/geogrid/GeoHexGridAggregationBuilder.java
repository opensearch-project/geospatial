/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexHelper.checkPrecisionRange;

import java.io.IOException;
import java.util.Map;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregatorSupplier;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;

/**
 * Aggregation Builder for geo hex grid
 */
public class GeoHexGridAggregationBuilder extends GeoGridAggregationBuilder {

    /**
     * Aggregation context name
     */
    public static final String NAME = "geohex_grid";
    public static final ValuesSourceRegistry.RegistryKey<GeoGridAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        GeoGridAggregatorSupplier.class
    );
    public static final ObjectParser<GeoHexGridAggregationBuilder, String> PARSER = createParser(
        NAME,
        GeoHexGridAggregationBuilder::parsePrecision,
        GeoHexGridAggregationBuilder::new
    );
    private static final int DEFAULT_MAX_NUM_CELLS = 10000;
    private static final int DEFAULT_PRECISION = 5;
    private static final int DEFAULT_SHARD_SIZE = -1;

    public GeoHexGridAggregationBuilder(String name) {
        super(name);
        precision(DEFAULT_PRECISION);
        size(DEFAULT_MAX_NUM_CELLS);
        shardSize = DEFAULT_SHARD_SIZE;
    }

    public GeoHexGridAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Register's Geo Hex Aggregation
     * @param builder Builder to register new Aggregation
     */
    public static void registerAggregators(final ValuesSourceRegistry.Builder builder) {
        GeoHexGridAggregatorFactory.registerAggregators(builder);
    }

    @Override
    public GeoGridAggregationBuilder precision(int precision) {
        checkPrecisionRange(precision);
        this.precision = precision;
        return this;
    }

    protected GeoHexGridAggregationBuilder(
        GeoGridAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceAggregatorFactory createFactory(
        String name,
        ValuesSourceConfig config,
        int precision,
        int requiredSize,
        int shardSize,
        GeoBoundingBox geoBoundingBox,
        QueryShardContext queryShardContext,
        AggregatorFactory aggregatorFactory,
        AggregatorFactories.Builder builder,
        Map<String, Object> metadata
    ) throws IOException {
        return new GeoHexGridAggregatorFactory(
            name,
            config,
            precision,
            requiredSize,
            shardSize,
            geoBoundingBox,
            queryShardContext,
            aggregatorFactory,
            builder,
            metadata
        );
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder builder, Map<String, Object> metadata) {
        return new GeoHexGridAggregationBuilder(this, builder, metadata);
    }

    private static int parsePrecision(final XContentParser parser) throws IOException, OpenSearchParseException {
        final var token = parser.currentToken();
        if (token.equals(XContentParser.Token.VALUE_NUMBER)) {
            return XContentMapValues.nodeIntegerValue(parser.intValue());
        }
        final var precision = parser.text();
        return XContentMapValues.nodeIntegerValue(precision);
    }
}
