/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opensearch.OpenSearchException;
import org.opensearch.SpecialPermission;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.MultiSearchRequestBuilder;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;

/**
 * Helper class for GeoIp data
 */
@Log4j2
public class GeoIpDataHelper {
    private static final String IP_RANGE_FIELD_NAME = "_cidr";
    private static final String DATA_FIELD_NAME = "_data";
    private static final Tuple<String, Integer> INDEX_SETTING_NUM_OF_SHARDS = new Tuple<>("index.number_of_shards", 1);
    private static final Tuple<String, String> INDEX_SETTING_AUTO_EXPAND_REPLICAS = new Tuple<>("index.auto_expand_replicas", "0-all");

    /**
     * Create an index of single shard with auto expand replicas to all nodes
     *
     * @param clusterService cluster service
     * @param client client
     * @param indexName index name
     * @param timeout timeout
     */
    public static void createIndexIfNotExists(
        final ClusterService clusterService,
        final Client client,
        final String indexName,
        final TimeValue timeout
    ) {
        if (clusterService.state().metadata().hasIndex(indexName) == true) {
            return;
        }
        final Map<String, Object> indexSettings = new HashMap<>();
        indexSettings.put(INDEX_SETTING_NUM_OF_SHARDS.v1(), INDEX_SETTING_NUM_OF_SHARDS.v2());
        indexSettings.put(INDEX_SETTING_AUTO_EXPAND_REPLICAS.v1(), INDEX_SETTING_AUTO_EXPAND_REPLICAS.v2());
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(indexSettings).mapping(getIndexMapping());
        client.admin().indices().create(createIndexRequest).actionGet(timeout);
    }

    /**
     * Generate XContentBuilder representing datasource database index mapping
     *
     * {
     *     "dynamic": false,
     *     "properties": {
     *         "_cidr": {
     *             "type": "ip_range",
     *             "doc_values": false
     *         }
     *     }
     * }
     *
     * @return String representing datasource database index mapping
     */
    private static String getIndexMapping() {
        try {
            try (InputStream is = DatasourceHelper.class.getResourceAsStream("/mappings/ip2geo_datasource.json")) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    return reader.lines().collect(Collectors.joining());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create CSVParser of a GeoIP data
     *
     * @param manifest Datasource manifest
     * @return CSVParser for GeoIP data
     */
    @SuppressForbidden(reason = "Need to connect to http endpoint to read GeoIP database file")
    public static CSVParser getDatabaseReader(final DatasourceManifest manifest) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<CSVParser>) () -> {
            try {
                URL zipUrl = new URL(manifest.getUrl());
                ZipInputStream zipIn = new ZipInputStream(zipUrl.openStream());
                ZipEntry zipEntry = zipIn.getNextEntry();
                while (zipEntry != null) {
                    if (!zipEntry.getName().equalsIgnoreCase(manifest.getDbName())) {
                        zipEntry = zipIn.getNextEntry();
                        continue;
                    }
                    return new CSVParser(new BufferedReader(new InputStreamReader(zipIn)), CSVFormat.RFC4180);
                }
            } catch (IOException e) {
                throw new OpenSearchException("failed to read geoip data from {}", manifest.getUrl(), e);
            }
            throw new OpenSearchException(
                "database file [{}] does not exist in the zip file [{}]",
                manifest.getDbName(),
                manifest.getUrl()
            );
        });
    }

    /**
     * Create a document in json string format to ingest in datasource database index
     *
     * It assumes the first field as ip_range. The rest is added under data field.
     *
     * Document example
     * {
     *   "_cidr":"1.0.0.1/25",
     *   "_data":{
     *       "country": "USA",
     *       "city": "Seattle",
     *       "location":"13.23,42.12"
     *   }
     * }
     *
     * @param fields a list of field name
     * @param values a list of values
     * @return Document in json string format
     */
    public static String createDocument(final String[] fields, final String[] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"");
        sb.append(IP_RANGE_FIELD_NAME);
        sb.append("\":\"");
        sb.append(values[0]);
        sb.append("\",\"");
        sb.append(DATA_FIELD_NAME);
        sb.append("\":{");
        for (int i = 1; i < fields.length; i++) {
            if (i != 1) {
                sb.append(",");
            }
            sb.append("\"");
            sb.append(fields[i]);
            sb.append("\":\"");
            sb.append(values[i]);
            sb.append("\"");
        }
        sb.append("}}");
        return sb.toString();
    }

    /**
     * Query a given index using a given ip address to get geo data
     *
     * @param client client
     * @param indexName index
     * @param ip ip address
     * @param actionListener action listener
     */
    public static void getGeoData(
        final Client client,
        final String indexName,
        final String ip,
        final ActionListener<Map<String, Object>> actionListener
    ) {
        client.prepareSearch(indexName)
            .setSize(1)
            .setQuery(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip))
            .setPreference("_local")
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(final SearchResponse searchResponse) {
                    if (searchResponse.getHits().getHits().length == 0) {
                        actionListener.onResponse(Collections.emptyMap());
                    } else {
                        Map<String, Object> geoData = (Map<String, Object>) XContentHelper.convertToMap(
                            searchResponse.getHits().getAt(0).getSourceRef(),
                            false,
                            XContentType.JSON
                        ).v2().get(DATA_FIELD_NAME);
                        actionListener.onResponse(geoData);
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    actionListener.onFailure(e);
                }
            });
    }

    /**
     * Query a given index using a given ip address iterator to get geo data
     *
     * This method calls itself recursively until it processes all ip addresses in bulk of {@code bulkSize}.
     *
     * @param client the client
     * @param indexName the index name
     * @param ipIterator the iterator of ip addresses
     * @param maxBundleSize number of ip address to pass in multi search
     * @param maxConcurrentSearches the max concurrent search requests
     * @param firstOnly return only the first matching result if true
     * @param geoData collected geo data
     * @param actionListener the action listener
     */
    public static void getGeoData(
        final Client client,
        final String indexName,
        final Iterator<String> ipIterator,
        final Integer maxBundleSize,
        final Integer maxConcurrentSearches,
        final boolean firstOnly,
        final Map<String, Map<String, Object>> geoData,
        final ActionListener<Object> actionListener
    ) {
        MultiSearchRequestBuilder mRequestBuilder = client.prepareMultiSearch();
        if (maxConcurrentSearches != 0) {
            mRequestBuilder.setMaxConcurrentSearchRequests(maxConcurrentSearches);
        }

        List<String> ipsToSearch = new ArrayList<>(maxBundleSize);
        while (ipIterator.hasNext() && ipsToSearch.size() < maxBundleSize) {
            String ip = ipIterator.next();
            if (geoData.get(ip) == null) {
                mRequestBuilder.add(
                    client.prepareSearch(indexName)
                        .setSize(1)
                        .setQuery(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip))
                        .setPreference("_local")
                );
                ipsToSearch.add(ip);
            }
        }

        if (ipsToSearch.isEmpty()) {
            actionListener.onResponse(null);
            return;
        }

        mRequestBuilder.execute(new ActionListener<>() {
            @Override
            public void onResponse(final MultiSearchResponse items) {
                for (int i = 0; i < ipsToSearch.size(); i++) {
                    if (items.getResponses()[i].isFailure()) {
                        actionListener.onFailure(items.getResponses()[i].getFailure());
                        return;
                    }

                    if (items.getResponses()[i].getResponse().getHits().getHits().length == 0) {
                        geoData.put(ipsToSearch.get(i), Collections.emptyMap());
                        continue;
                    }

                    Map<String, Object> data = (Map<String, Object>) XContentHelper.convertToMap(
                        items.getResponses()[i].getResponse().getHits().getAt(0).getSourceRef(),
                        false,
                        XContentType.JSON
                    ).v2().get(DATA_FIELD_NAME);

                    geoData.put(ipsToSearch.get(i), data);

                    if (firstOnly) {
                        actionListener.onResponse(null);
                        return;
                    }
                }
                getGeoData(client, indexName, ipIterator, maxBundleSize, maxConcurrentSearches, firstOnly, geoData, actionListener);
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    /**
     * Puts GeoIP data from CSVRecord iterator into a given index in bulk
     *
     * @param client OpenSearch client
     * @param indexName Index name to puts the GeoIP data
     * @param fields Field name matching with data in CSVRecord in order
     * @param iterator GeoIP data to insert
     * @param bulkSize Bulk size of data to process
     * @param timeout Timeout
     */
    public static void putGeoData(
        final Client client,
        final String indexName,
        final String[] fields,
        final Iterator<CSVRecord> iterator,
        final int bulkSize,
        final TimeValue timeout
    ) {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            String document = createDocument(fields, record.values());
            IndexRequest request = Requests.indexRequest(indexName).source(document, XContentType.JSON);
            bulkRequest.add(request);
            if (!iterator.hasNext() || bulkRequest.requests().size() == bulkSize) {
                BulkResponse response = client.bulk(bulkRequest).actionGet(timeout);
                if (response.hasFailures()) {
                    throw new OpenSearchException(
                        "error occurred while ingesting GeoIP data in {} with an error {}",
                        indexName,
                        response.buildFailureMessage()
                    );
                }
                bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            }
        }
        client.admin().indices().prepareRefresh(indexName).execute().actionGet(timeout);
        client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute().actionGet(timeout);
    }
}
