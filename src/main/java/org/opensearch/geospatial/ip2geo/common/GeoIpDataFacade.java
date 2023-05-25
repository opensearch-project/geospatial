/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.OpenSearchException;
import org.opensearch.SpecialPermission;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.MultiSearchRequestBuilder;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.constants.IndexSetting;
import org.opensearch.geospatial.shared.Constants;
import org.opensearch.geospatial.shared.StashedThreadContext;
import org.opensearch.index.query.QueryBuilders;

/**
 * Facade class for GeoIp data
 */
@Log4j2
public class GeoIpDataFacade {
    private static final String IP_RANGE_FIELD_NAME = "_cidr";
    private static final String DATA_FIELD_NAME = "_data";
    private static final Map<String, Object> INDEX_SETTING_TO_CREATE = Map.of(
        IndexSetting.NUMBER_OF_SHARDS,
        1,
        IndexSetting.NUMBER_OF_REPLICAS,
        0,
        IndexSetting.REFRESH_INTERVAL,
        -1,
        IndexSetting.HIDDEN,
        true
    );
    private static final Map<String, Object> INDEX_SETTING_TO_FREEZE = Map.of(
        IndexSetting.AUTO_EXPAND_REPLICAS,
        "0-all",
        IndexSetting.BLOCKS_WRITE,
        true
    );
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;
    private final Client client;

    public GeoIpDataFacade(final ClusterService clusterService, final Client client) {
        this.clusterService = clusterService;
        this.clusterSettings = clusterService.getClusterSettings();
        this.client = client;
    }

    /**
     * Create an index for GeoIP data
     *
     * Index setting start with single shard, zero replica, no refresh interval, and hidden.
     * Once the GeoIP data is indexed, do refresh and force merge.
     * Then, change the index setting to expand replica to all nodes, and read only allow delete.
     * See {@link #freezeIndex}
     *
     * @param indexName index name
     */
    public void createIndexIfNotExists(final String indexName) {
        if (clusterService.state().metadata().hasIndex(indexName) == true) {
            return;
        }
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(INDEX_SETTING_TO_CREATE)
            .mapping(getIndexMapping());
        StashedThreadContext.run(
            client,
            () -> client.admin().indices().create(createIndexRequest).actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT))
        );
    }

    private void freezeIndex(final String indexName) {
        TimeValue timeout = clusterSettings.get(Ip2GeoSettings.TIMEOUT);
        StashedThreadContext.run(client, () -> {
            client.admin().indices().prepareRefresh(indexName).execute().actionGet(timeout);
            client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute().actionGet(timeout);
            client.admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(INDEX_SETTING_TO_FREEZE)
                .execute()
                .actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT));
        });
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
    private String getIndexMapping() {
        try {
            try (InputStream is = DatasourceFacade.class.getResourceAsStream("/mappings/ip2geo_geoip.json")) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    return reader.lines().map(String::trim).collect(Collectors.joining());
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
    public CSVParser getDatabaseReader(final DatasourceManifest manifest) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<CSVParser>) () -> {
            try {
                URL zipUrl = new URL(manifest.getUrl());
                return internalGetDatabaseReader(manifest, zipUrl.openConnection());
            } catch (IOException e) {
                throw new OpenSearchException("failed to read geoip data from {}", manifest.getUrl(), e);
            }
        });
    }

    @VisibleForTesting
    @SuppressForbidden(reason = "Need to connect to http endpoint to read GeoIP database file")
    protected CSVParser internalGetDatabaseReader(final DatasourceManifest manifest, final URLConnection connection) throws IOException {
        connection.addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
        ZipInputStream zipIn = new ZipInputStream(connection.getInputStream());
        ZipEntry zipEntry = zipIn.getNextEntry();
        while (zipEntry != null) {
            if (zipEntry.getName().equalsIgnoreCase(manifest.getDbName()) == false) {
                zipEntry = zipIn.getNextEntry();
                continue;
            }
            return new CSVParser(new BufferedReader(new InputStreamReader(zipIn)), CSVFormat.RFC4180);
        }
        throw new OpenSearchException("database file [{}] does not exist in the zip file [{}]", manifest.getDbName(), manifest.getUrl());
    }

    /**
     * Create a document to ingest in datasource database index
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
     * @throws IOException the exception
     */
    public XContentBuilder createDocument(final String[] fields, final String[] values) throws IOException {
        if (fields.length != values.length) {
            throw new OpenSearchException("header[{}] and record[{}] length does not match", fields, values);
        }
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(IP_RANGE_FIELD_NAME, values[0]);
        builder.startObject(DATA_FIELD_NAME);
        for (int i = 1; i < fields.length; i++) {
            if (Strings.isBlank(values[i])) {
                continue;
            }
            builder.field(fields[i], values[i]);
        }
        builder.endObject();
        builder.endObject();
        builder.close();
        return builder;
    }

    /**
     * Query a given index using a given ip address to get geoip data
     *
     * @param indexName index
     * @param ip ip address
     * @param actionListener action listener
     */
    public void getGeoIpData(final String indexName, final String ip, final ActionListener<Map<String, Object>> actionListener) {
        StashedThreadContext.run(
            client,
            () -> client.prepareSearch(indexName)
                .setSize(1)
                .setQuery(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip))
                .setPreference("_local")
                .setRequestCache(true)
                .execute(new ActionListener<>() {
                    @Override
                    public void onResponse(final SearchResponse searchResponse) {
                        if (searchResponse.getHits().getHits().length == 0) {
                            actionListener.onResponse(Collections.emptyMap());
                        } else {
                            Map<String, Object> geoIpData = (Map<String, Object>) XContentHelper.convertToMap(
                                searchResponse.getHits().getAt(0).getSourceRef(),
                                false,
                                XContentType.JSON
                            ).v2().get(DATA_FIELD_NAME);
                            actionListener.onResponse(geoIpData);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        actionListener.onFailure(e);
                    }
                })
        );
    }

    /**
     * Query a given index using a given ip address iterator to get geoip data
     *
     * This method calls itself recursively until it processes all ip addresses in bulk of {@code bulkSize}.
     *
     * @param indexName the index name
     * @param ipIterator the iterator of ip addresses
     * @param maxBundleSize number of ip address to pass in multi search
     * @param maxConcurrentSearches the max concurrent search requests
     * @param firstOnly return only the first matching result if true
     * @param geoIpData collected geo data
     * @param actionListener the action listener
     */
    public void getGeoIpData(
        final String indexName,
        final Iterator<String> ipIterator,
        final Integer maxBundleSize,
        final Integer maxConcurrentSearches,
        final boolean firstOnly,
        final Map<String, Map<String, Object>> geoIpData,
        final ActionListener<Map<String, Map<String, Object>>> actionListener
    ) {
        MultiSearchRequestBuilder mRequestBuilder = client.prepareMultiSearch();
        if (maxConcurrentSearches != 0) {
            mRequestBuilder.setMaxConcurrentSearchRequests(maxConcurrentSearches);
        }

        List<String> ipsToSearch = new ArrayList<>(maxBundleSize);
        while (ipIterator.hasNext() && ipsToSearch.size() < maxBundleSize) {
            String ip = ipIterator.next();
            if (geoIpData.get(ip) == null) {
                mRequestBuilder.add(
                    client.prepareSearch(indexName)
                        .setSize(1)
                        .setQuery(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip))
                        .setPreference("_local")
                        .setRequestCache(true)
                );
                ipsToSearch.add(ip);
            }
        }

        if (ipsToSearch.isEmpty()) {
            actionListener.onResponse(geoIpData);
            return;
        }

        StashedThreadContext.run(client, () -> mRequestBuilder.execute(new ActionListener<>() {
            @Override
            public void onResponse(final MultiSearchResponse items) {
                for (int i = 0; i < ipsToSearch.size(); i++) {
                    if (items.getResponses()[i].isFailure()) {
                        actionListener.onFailure(items.getResponses()[i].getFailure());
                        return;
                    }

                    if (items.getResponses()[i].getResponse().getHits().getHits().length == 0) {
                        geoIpData.put(ipsToSearch.get(i), Collections.emptyMap());
                        continue;
                    }

                    Map<String, Object> data = (Map<String, Object>) XContentHelper.convertToMap(
                        items.getResponses()[i].getResponse().getHits().getAt(0).getSourceRef(),
                        false,
                        XContentType.JSON
                    ).v2().get(DATA_FIELD_NAME);

                    geoIpData.put(ipsToSearch.get(i), data);

                    if (firstOnly) {
                        actionListener.onResponse(geoIpData);
                        return;
                    }
                }
                getGeoIpData(indexName, ipIterator, maxBundleSize, maxConcurrentSearches, firstOnly, geoIpData, actionListener);
            }

            @Override
            public void onFailure(final Exception e) {
                actionListener.onFailure(e);
            }
        }));
    }

    /**
     * Puts GeoIP data from CSVRecord iterator into a given index in bulk
     *
     * @param indexName Index name to puts the GeoIP data
     * @param fields Field name matching with data in CSVRecord in order
     * @param iterator GeoIP data to insert
     * @param bulkSize Bulk size of data to process
     * @param renewLock Runnable to renew lock
     */
    public void putGeoIpData(
        @NonNull final String indexName,
        @NonNull final String[] fields,
        @NonNull final Iterator<CSVRecord> iterator,
        final int bulkSize,
        @NonNull final Runnable renewLock
    ) throws IOException {
        TimeValue timeout = clusterSettings.get(Ip2GeoSettings.TIMEOUT);
        final BulkRequest bulkRequest = new BulkRequest();
        Queue<DocWriteRequest> requests = new LinkedList<>();
        for (int i = 0; i < bulkSize; i++) {
            requests.add(Requests.indexRequest(indexName));
        }
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            XContentBuilder document = createDocument(fields, record.values());
            IndexRequest indexRequest = (IndexRequest) requests.poll();
            indexRequest.source(document);
            indexRequest.id(record.get(0));
            bulkRequest.add(indexRequest);
            if (iterator.hasNext() == false || bulkRequest.requests().size() == bulkSize) {
                BulkResponse response = StashedThreadContext.run(client, () -> client.bulk(bulkRequest).actionGet(timeout));
                if (response.hasFailures()) {
                    throw new OpenSearchException(
                        "error occurred while ingesting GeoIP data in {} with an error {}",
                        indexName,
                        response.buildFailureMessage()
                    );
                }
                requests.addAll(bulkRequest.requests());
                bulkRequest.requests().clear();
            }
            renewLock.run();
        }
        freezeIndex(indexName);
    }

    public AcknowledgedResponse deleteIp2GeoDataIndex(final String index) {
        if (index == null || index.startsWith(IP2GEO_DATA_INDEX_NAME_PREFIX) == false) {
            throw new OpenSearchException(
                "the index[{}] is not ip2geo data index which should start with {}",
                index,
                IP2GEO_DATA_INDEX_NAME_PREFIX
            );
        }
        return StashedThreadContext.run(
            client,
            () -> client.admin()
                .indices()
                .prepareDelete(index)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .execute()
                .actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT))
        );
    }
}