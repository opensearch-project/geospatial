/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

import lombok.extern.log4j.Log4j2;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.exceptions.ConcurrentModificationException;
import org.opensearch.geospatial.exceptions.IncompatibleDatasourceException;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceTask;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceUpdateService;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action to update datasource
 */
@Log4j2
public class UpdateDatasourceTransportAction extends HandledTransportAction<UpdateDatasourceRequest, AcknowledgedResponse> {
    private static final long LOCK_DURATION_IN_SECONDS = 300l;
    private final Ip2GeoLockService lockService;
    private final DatasourceFacade datasourceFacade;
    private final DatasourceUpdateService datasourceUpdateService;
    private final ThreadPool threadPool;

    /**
     * Constructor
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param lockService the lock service
     * @param datasourceFacade the datasource facade
     * @param datasourceUpdateService the datasource update service
     */
    @Inject
    public UpdateDatasourceTransportAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Ip2GeoLockService lockService,
        final DatasourceFacade datasourceFacade,
        final DatasourceUpdateService datasourceUpdateService,
        final ThreadPool threadPool
    ) {
        super(UpdateDatasourceAction.NAME, transportService, actionFilters, UpdateDatasourceRequest::new);
        this.lockService = lockService;
        this.datasourceUpdateService = datasourceUpdateService;
        this.datasourceFacade = datasourceFacade;
        this.threadPool = threadPool;
    }

    /**
     * Get a lock and update datasource
     *
     * @param task the task
     * @param request the request
     * @param listener the listener
     */
    @Override
    protected void doExecute(final Task task, final UpdateDatasourceRequest request, final ActionListener<AcknowledgedResponse> listener) {
        lockService.acquireLock(request.getName(), LOCK_DURATION_IN_SECONDS, ActionListener.wrap(lock -> {
            if (lock == null) {
                listener.onFailure(
                    new ConcurrentModificationException("another processor is holding a lock on the resource. Try again later")
                );
                return;
            }
            try {
                // TODO: makes every sub-methods as async call to avoid using a thread in generic pool
                threadPool.generic().submit(() -> {
                    try {
                        Datasource datasource = datasourceFacade.getDatasource(request.getName());
                        if (datasource == null) {
                            throw new ResourceNotFoundException("no such datasource exist");
                        }
                        validate(request, datasource);
                        updateIfChanged(request, datasource);
                        lockService.releaseLock(lock);
                        listener.onResponse(new AcknowledgedResponse(true));
                    } catch (Exception e) {
                        lockService.releaseLock(lock);
                        listener.onFailure(e);
                    }
                });
            } catch (Exception e) {
                lockService.releaseLock(lock);
                listener.onFailure(e);
            }
        }, exception -> listener.onFailure(exception)));
    }

    private void updateIfChanged(final UpdateDatasourceRequest request, final Datasource datasource) {
        boolean isChanged = false;
        if (isEndpointChanged(request, datasource)) {
            datasource.setEndpoint(request.getEndpoint());
            isChanged = true;
        }
        if (isUpdateIntervalChanged(request)) {
            datasource.setUserSchedule(new IntervalSchedule(Instant.now(), (int) request.getUpdateInterval().getDays(), ChronoUnit.DAYS));
            datasource.setSystemSchedule(datasource.getUserSchedule());
            datasource.setTask(DatasourceTask.ALL);
            isChanged = true;
        }

        if (isChanged) {
            datasourceFacade.updateDatasource(datasource);
        }
    }

    /**
     * Additional validation based on an existing datasource
     *
     * Basic validation is done in UpdateDatasourceRequest#validate
     * In this method we do additional validation based on an existing datasource
     *
     * 1. Check the compatibility of new fields and old fields
     * 2. Check the updateInterval is less than validForInDays in datasource
     *
     * This method throws exception if one of validation fails.
     *
     * @param request the update request
     * @param datasource the existing datasource
     * @throws IOException the exception
     */
    private void validate(final UpdateDatasourceRequest request, final Datasource datasource) throws IOException {
        validateFieldsCompatibility(request, datasource);
        validateUpdateIntervalIsLessThanValidForInDays(request, datasource);
        validateNextUpdateScheduleIsBeforeExpirationDay(request, datasource);
    }

    private void validateNextUpdateScheduleIsBeforeExpirationDay(final UpdateDatasourceRequest request, final Datasource datasource) {
        if (request.getUpdateInterval() == null) {
            return;
        }

        IntervalSchedule newSchedule = new IntervalSchedule(Instant.now(), (int) request.getUpdateInterval().getDays(), ChronoUnit.DAYS);

        if (newSchedule.getNextExecutionTime(Instant.now()).isAfter(datasource.expirationDay())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "datasource will expire at %s with the update interval", datasource.expirationDay().toString())
            );
        }
    }

    private void validateFieldsCompatibility(final UpdateDatasourceRequest request, final Datasource datasource) throws IOException {
        if (isEndpointChanged(request, datasource) == false) {
            return;
        }

        List<String> fields = datasourceUpdateService.getHeaderFields(request.getEndpoint());
        if (datasource.isCompatible(fields) == false) {
            throw new IncompatibleDatasourceException(
                "new fields [{}] does not contain all old fields [{}]",
                fields.toString(),
                datasource.getDatabase().getFields().toString()
            );
        }
    }

    private void validateUpdateIntervalIsLessThanValidForInDays(final UpdateDatasourceRequest request, final Datasource datasource)
        throws IOException {
        if (isEndpointChanged(request, datasource) == false && isUpdateIntervalChanged(request) == false) {
            return;
        }

        long validForInDays = isEndpointChanged(request, datasource)
            ? DatasourceManifest.Builder.build(new URL(request.getEndpoint())).getValidForInDays()
            : datasource.getDatabase().getValidForInDays();

        long updateInterval = isUpdateIntervalChanged(request)
            ? request.getUpdateInterval().days()
            : datasource.getUserSchedule().getInterval();

        if (updateInterval >= validForInDays) {
            throw new InvalidParameterException(
                String.format(Locale.ROOT, "updateInterval %d should be smaller than %d", updateInterval, validForInDays)
            );
        }
    }

    private boolean isEndpointChanged(final UpdateDatasourceRequest request, final Datasource datasource) {
        return request.getEndpoint() != null && request.getEndpoint().equals(datasource.getEndpoint()) == false;
    }

    /**
     * Update interval is changed as long as user provide one because
     * start time will get updated even if the update interval is same as current one.
     *
     * @param request the update datasource request
     * @return true if update interval is changed, and false otherwise
     */
    private boolean isUpdateIntervalChanged(final UpdateDatasourceRequest request) {
        return request.getUpdateInterval() != null;
    }
}
