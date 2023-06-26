/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.dao;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.shared.Constants;
import org.opensearch.geospatial.shared.StashedThreadContext;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Data access object  for GeoIp data
 */
@Log4j2
public class GeoIpDataDao {
    public static final int BUNDLE_SIZE = 128;
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

    public GeoIpDataDao(final ClusterService clusterService, final Client client) {
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
            client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute().actionGet(timeout);
            client.admin().indices().prepareRefresh(indexName).execute().actionGet(timeout);
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
            try (InputStream is = DatasourceDao.class.getResourceAsStream("/mappings/ip2geo_geoip.json")) {
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
                        try {
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
                        } catch (Exception e) {
                            actionListener.onFailure(e);
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
     * Query a given index using a given list of ip addresses to get geoip data
     *
     * @param indexName index
     * @param ips list of ip addresses
     * @param actionListener action listener
     */
    public void getGeoIpData(
        final String indexName,
        final List<String> ips,
        final ActionListener<List<Map<String, Object>>> actionListener
    ) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        ips.stream().forEach(ip -> boolQueryBuilder.should(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip)));
        StashedThreadContext.run(
            client,
            () -> client.prepareSearch(indexName)
                .setSize(ips.size())
                .setQuery(boolQueryBuilder)
                .setPreference("_local")
                .setRequestCache(true)
                .execute(new ActionListener<>() {
                    @Override
                    public void onResponse(final SearchResponse searchResponse) {
                        try {
                            actionListener.onResponse(toGeoIpDataList(searchResponse));
                        } catch (Exception e) {
                            actionListener.onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        actionListener.onFailure(e);
                    }
                })
        );
    }

    private List<Map<String, Object>> toGeoIpDataList(final SearchResponse searchResponse) {
        if (searchResponse.getHits().getHits().length == 0) {
            return Collections.emptyList();
        }

        return Arrays.stream(searchResponse.getHits().getHits())
            .map(
                data -> (Map<String, Object>) XContentHelper.convertToMap(data.getSourceRef(), false, XContentType.JSON)
                    .v2()
                    .get(DATA_FIELD_NAME)
            )
            .collect(Collectors.toList());
    }

    /**
     * Puts GeoIP data from CSVRecord iterator into a given index in bulk
     *
     * @param indexName Index name to puts the GeoIP data
     * @param fields Field name matching with data in CSVRecord in order
     * @param iterator GeoIP data to insert
     * @param renewLock Runnable to renew lock
     */
    public void putGeoIpData(
        @NonNull final String indexName,
        @NonNull final String[] fields,
        @NonNull final Iterator<CSVRecord> iterator,
        @NonNull final Runnable renewLock
    ) throws IOException {
        TimeValue timeout = clusterSettings.get(Ip2GeoSettings.TIMEOUT);
        final BulkRequest bulkRequest = new BulkRequest();
        Queue<DocWriteRequest> requests = new LinkedList<>();
        for (int i = 0; i < BUNDLE_SIZE; i++) {
            requests.add(Requests.indexRequest(indexName));
        }
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            XContentBuilder document = createDocument(fields, record.values());
            IndexRequest indexRequest = (IndexRequest) requests.poll();
            indexRequest.source(document);
            indexRequest.id(record.get(0));
            bulkRequest.add(indexRequest);
            if (iterator.hasNext() == false || bulkRequest.requests().size() == BUNDLE_SIZE) {
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

    public void deleteIp2GeoDataIndex(final String index) {
        deleteIp2GeoDataIndex(Arrays.asList(index));
    }

    public void deleteIp2GeoDataIndex(final List<String> indices) {
        if (indices == null || indices.isEmpty()) {
            return;
        }

        Optional<String> invalidIndex = indices.stream()
            .filter(index -> index.startsWith(IP2GEO_DATA_INDEX_NAME_PREFIX) == false)
            .findAny();
        if (invalidIndex.isPresent()) {
            throw new OpenSearchException(
                "the index[{}] is not ip2geo data index which should start with {}",
                invalidIndex.get(),
                IP2GEO_DATA_INDEX_NAME_PREFIX
            );
        }

        AcknowledgedResponse response = StashedThreadContext.run(
            client,
            () -> client.admin()
                .indices()
                .prepareDelete(indices.toArray(new String[0]))
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                .execute()
                .actionGet(clusterSettings.get(Ip2GeoSettings.TIMEOUT))
        );

        if (response.isAcknowledged() == false) {
            throw new OpenSearchException("failed to delete data[{}] in datasource", String.join(",", indices));
        }
    }
}
