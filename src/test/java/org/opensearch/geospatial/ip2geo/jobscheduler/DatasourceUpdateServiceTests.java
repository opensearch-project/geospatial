/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import lombok.SneakyThrows;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

@SuppressForbidden(reason = "unit test")
public class DatasourceUpdateServiceTests extends Ip2GeoTestCase {
    private DatasourceUpdateService datasourceUpdateService;

    @Before
    public void init() {
        datasourceUpdateService = new DatasourceUpdateService(clusterService, datasourceFacade, geoIpDataFacade);
    }

    @SneakyThrows
    public void testUpdateOrCreateGeoIpData_whenHashValueIsSame_thenSkipUpdate() {
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(manifestFile.toURI().toURL());

        Datasource datasource = new Datasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.getUpdateStats().setLastSkippedAt(null);
        datasource.getDatabase().setUpdatedAt(Instant.ofEpochMilli(manifest.getUpdatedAt()));
        datasource.getDatabase().setSha256Hash(manifest.getSha256Hash());
        datasource.setEndpoint(manifestFile.toURI().toURL().toExternalForm());

        // Run
        datasourceUpdateService.updateOrCreateGeoIpData(datasource, mock(Runnable.class));

        // Verify
        assertNotNull(datasource.getUpdateStats().getLastSkippedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }

    @SneakyThrows
    public void testUpdateOrCreateGeoIpData_whenInvalidData_thenThrowException() {
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(manifestFile.toURI().toURL());

        File sampleFile = new File(
            this.getClass().getClassLoader().getResource("ip2geo/sample_invalid_less_than_two_fields.csv").getFile()
        );
        when(geoIpDataFacade.getDatabaseReader(any())).thenReturn(CSVParser.parse(sampleFile, StandardCharsets.UTF_8, CSVFormat.RFC4180));

        Datasource datasource = new Datasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.getDatabase().setUpdatedAt(Instant.ofEpochMilli(manifest.getUpdatedAt() - 1));
        datasource.getDatabase().setSha256Hash(manifest.getSha256Hash().substring(1));
        datasource.getDatabase().setFields(Arrays.asList("country_name"));
        datasource.setEndpoint(manifestFile.toURI().toURL().toExternalForm());

        // Run
        expectThrows(OpenSearchException.class, () -> datasourceUpdateService.updateOrCreateGeoIpData(datasource, mock(Runnable.class)));
    }

    @SneakyThrows
    public void testUpdateOrCreateGeoIpData_whenIncompatibleFields_thenThrowException() {
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(manifestFile.toURI().toURL());

        File sampleFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.csv").getFile());
        when(geoIpDataFacade.getDatabaseReader(any())).thenReturn(CSVParser.parse(sampleFile, StandardCharsets.UTF_8, CSVFormat.RFC4180));

        Datasource datasource = new Datasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.getDatabase().setUpdatedAt(Instant.ofEpochMilli(manifest.getUpdatedAt() - 1));
        datasource.getDatabase().setSha256Hash(manifest.getSha256Hash().substring(1));
        datasource.getDatabase().setFields(Arrays.asList("country_name", "additional_field"));
        datasource.setEndpoint(manifestFile.toURI().toURL().toExternalForm());

        // Run
        expectThrows(OpenSearchException.class, () -> datasourceUpdateService.updateOrCreateGeoIpData(datasource, mock(Runnable.class)));
    }

    @SneakyThrows
    public void testUpdateOrCreateGeoIpData_whenValidInput_thenSucceed() {
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());
        DatasourceManifest manifest = DatasourceManifest.Builder.build(manifestFile.toURI().toURL());

        File sampleFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.csv").getFile());
        when(geoIpDataFacade.getDatabaseReader(any())).thenReturn(CSVParser.parse(sampleFile, StandardCharsets.UTF_8, CSVFormat.RFC4180));

        Datasource datasource = new Datasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.getDatabase().setUpdatedAt(Instant.ofEpochMilli(manifest.getUpdatedAt() - 1));
        datasource.getDatabase().setSha256Hash(manifest.getSha256Hash().substring(1));
        datasource.getDatabase().setFields(Arrays.asList("country_name"));
        datasource.setEndpoint(manifestFile.toURI().toURL().toExternalForm());
        datasource.getUpdateStats().setLastSucceededAt(null);
        datasource.getUpdateStats().setLastProcessingTimeInMillis(null);

        // Run
        datasourceUpdateService.updateOrCreateGeoIpData(datasource, mock(Runnable.class));

        // Verify
        assertEquals(manifest.getProvider(), datasource.getDatabase().getProvider());
        assertEquals(manifest.getSha256Hash(), datasource.getDatabase().getSha256Hash());
        assertEquals(Instant.ofEpochMilli(manifest.getUpdatedAt()), datasource.getDatabase().getUpdatedAt());
        assertEquals(manifest.getValidForInDays(), datasource.getDatabase().getValidForInDays());
        assertNotNull(datasource.getUpdateStats().getLastSucceededAt());
        assertNotNull(datasource.getUpdateStats().getLastProcessingTimeInMillis());
        verify(datasourceFacade, times(2)).updateDatasource(datasource);
        verify(geoIpDataFacade).putGeoIpData(
            eq(datasource.currentIndexName()),
            isA(String[].class),
            any(Iterator.class),
            anyInt(),
            any(Runnable.class)
        );
    }

    @SneakyThrows
    public void testGetHeaderFields_whenValidInput_thenReturnCorrectValue() {
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());

        File sampleFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.csv").getFile());
        when(geoIpDataFacade.getDatabaseReader(any())).thenReturn(CSVParser.parse(sampleFile, StandardCharsets.UTF_8, CSVFormat.RFC4180));

        // Run
        assertEquals(Arrays.asList("country_name"), datasourceUpdateService.getHeaderFields(manifestFile.toURI().toURL().toExternalForm()));
    }

    @SneakyThrows
    public void testDeleteUnusedIndices_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String indexPrefix = String.format(".ip2geo-data.%s.", datasourceName);
        Instant now = Instant.now();
        String currentIndex = indexPrefix + now.toEpochMilli();
        String oldIndex = indexPrefix + now.minusMillis(1).toEpochMilli();
        String lingeringIndex = indexPrefix + now.minusMillis(2).toEpochMilli();
        Datasource datasource = new Datasource();
        datasource.setName(datasourceName);
        datasource.getIndices().add(currentIndex);
        datasource.getIndices().add(oldIndex);
        datasource.getIndices().add(lingeringIndex);
        datasource.getDatabase().setUpdatedAt(now);

        when(metadata.hasIndex(currentIndex)).thenReturn(true);
        when(metadata.hasIndex(oldIndex)).thenReturn(true);
        when(metadata.hasIndex(lingeringIndex)).thenReturn(false);
        when(geoIpDataFacade.deleteIp2GeoDataIndex(any())).thenReturn(new AcknowledgedResponse(true));

        datasourceUpdateService.deleteUnusedIndices(datasource);

        assertEquals(1, datasource.getIndices().size());
        assertEquals(currentIndex, datasource.getIndices().get(0));
        verify(datasourceFacade).updateDatasource(datasource);
    }

    public void testUpdateDatasource_whenNoChange_thenNoUpdate() {
        Datasource datasource = randomDatasource();

        // Run
        datasourceUpdateService.updateDatasource(datasource, datasource.getSystemSchedule(), datasource.getTask());

        // Verify
        verify(datasourceFacade, never()).updateDatasource(any());
    }

    public void testUpdateDatasource_whenChange_thenUpdate() {
        Datasource datasource = randomDatasource();
        datasource.setTask(DatasourceTask.ALL);

        // Run
        datasourceUpdateService.updateDatasource(
            datasource,
            new IntervalSchedule(Instant.now(), datasource.getSystemSchedule().getInterval() + 1, ChronoUnit.DAYS),
            datasource.getTask()
        );
        datasourceUpdateService.updateDatasource(datasource, datasource.getSystemSchedule(), DatasourceTask.DELETE_UNUSED_INDICES);

        // Verify
        verify(datasourceFacade, times(2)).updateDatasource(any());
    }

    @SneakyThrows
    public void testGetHeaderFields_whenValidInput_thenSucceed() {
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());
        File sampleFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.csv").getFile());
        when(geoIpDataFacade.getDatabaseReader(any())).thenReturn(CSVParser.parse(sampleFile, StandardCharsets.UTF_8, CSVFormat.RFC4180));

        // Run
        List<String> fields = datasourceUpdateService.getHeaderFields(manifestFile.toURI().toURL().toExternalForm());

        // Verify
        List<String> expectedFields = Arrays.asList("country_name");
        assertEquals(expectedFields, fields);
    }
}
