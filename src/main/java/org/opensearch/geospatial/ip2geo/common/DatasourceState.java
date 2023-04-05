/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

/**
 * Ip2Geo datasource state
 *
 * When data source is created, it starts with PREPARING state. Once the first GeoIP data is generated, the state changes to AVAILABLE.
 * Only when the first GeoIP data generation failed, the state changes to FAILED.
 * Subsequent GeoIP data failure won't change data source state from AVAILABLE to FAILED.
 * When delete request is received, the data source state changes to DELETING.
 *
 * State changed from left to right for the entire lifecycle of a datasource
 * (PREPARING) to (FAILED or AVAILABLE) to (DELETING)
 *
 */
public enum DatasourceState {
    /**
     * Data source is being prepared
     */
    PREPARING,
    /**
     * Data source is ready to be used
     */
    AVAILABLE,
    /**
     * Data source preparation failed
     */
    FAILED,
    /**
     * Data source is being deleted
     */
    DELETING
}
