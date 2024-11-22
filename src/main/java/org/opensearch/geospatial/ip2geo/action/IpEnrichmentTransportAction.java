/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.geospatial.action.IpEnrichmentAction;
import org.opensearch.geospatial.action.IpEnrichmentRequest;
import org.opensearch.geospatial.action.IpEnrichmentResponse;
import org.opensearch.geospatial.ip2geo.dao.DatasourceDao;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoCachedDao;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Transport action to convert provided IP address String into GeoLocation data.
 */
@Log4j2
public class IpEnrichmentTransportAction extends HandledTransportAction<ActionRequest,
        ActionResponse> {

    private Ip2GeoCachedDao ip2GeoCachedDao;

    private String defaultDataSourceName;

    /**
     * Constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param cachedDao the cached datasource facade
     */
    @Inject
    public IpEnrichmentTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            Ip2GeoCachedDao cachedDao,
            DatasourceDao datasourceDao) {
        super(IpEnrichmentAction.NAME, transportService, actionFilters, IpEnrichmentRequest::new);
        this.ip2GeoCachedDao = cachedDao;
        List<Datasource> allDatasources = datasourceDao.getAllDatasources();
        this.defaultDataSourceName = (!allDatasources.isEmpty()) ? allDatasources.get(0).getName() : null;
    }


    /**
     * Overridden method to extract IP String from IpEnrichmentRequest object and return the enrichment result
     * in the form of IpEnrichmentResponse which contains the GeoLocation data for given IP String.
     * @param task the task.
     * @param request request object in the form of IpEnrichmentRequest which contain the IP String to resolve
     * @param listener a container which encapsulate IpEnrichmentResponse object with the GeoLocation data for given IP.
     */
    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        IpEnrichmentRequest enrichmentRequest = IpEnrichmentRequest.fromActionRequest(request);
        String ipString = enrichmentRequest.getIpString();
        if (enrichmentRequest.getDatasourceName() == null &&
            defaultDataSourceName == null) {
            log.error("No data source available, IpEnrichmentTransportAction aborted.");
            listener.onFailure(new IllegalArgumentException());
        } else {
            String dataSourceName = Optional.ofNullable(enrichmentRequest.getDatasourceName())
                    .orElse(defaultDataSourceName);
            String indexName = ip2GeoCachedDao.getIndexName(dataSourceName);
            Map<String, Object> geoLocationData = ip2GeoCachedDao.getGeoData(indexName, ipString);
            log.debug("GeoSpatial IP lookup on IP: [{}], and result [{}]", ipString, geoLocationData);
            listener.onResponse(new IpEnrichmentResponse(geoLocationData));
        }
    }

}
