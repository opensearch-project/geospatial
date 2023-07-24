/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.constants;

/**
 * Collection of keys for index setting
 */
public class IndexSetting {
    public static final String NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final String REFRESH_INTERVAL = "index.refresh_interval";
    public static final String AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final String HIDDEN = "index.hidden";
    public static final String BLOCKS_WRITE = "index.blocks.write";
}
