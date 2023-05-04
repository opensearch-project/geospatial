/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;

public class GetDatasourceResponseTests extends Ip2GeoTestCase {

    public void testStreamInOut_whenValidInput_thenSucceed() throws Exception {
        List<Datasource> datasourceList = Arrays.asList(randomDatasource(), randomDatasource());
        GetDatasourceResponse response = new GetDatasourceResponse(datasourceList);

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        GetDatasourceResponse copiedResponse = new GetDatasourceResponse(input);

        // Verify
        assertArrayEquals(response.getDatasources().toArray(), copiedResponse.getDatasources().toArray());
    }

    public void testToXContent_whenValidInput_thenSucceed() throws Exception {
        List<Datasource> datasourceList = Arrays.asList(randomDatasource(), randomDatasource());
        GetDatasourceResponse response = new GetDatasourceResponse(datasourceList);
        String json = Strings.toString(response.toXContent(JsonXContent.contentBuilder(), null));
        for (Datasource datasource : datasourceList) {
            assertTrue(json.contains(String.format(Locale.ROOT, "\"name\":\"%s\"", datasource.getName())));
            assertTrue(json.contains(String.format(Locale.ROOT, "\"state\":\"%s\"", datasource.getState())));
            assertTrue(json.contains(String.format(Locale.ROOT, "\"endpoint\":\"%s\"", datasource.getEndpoint())));
            assertTrue(json.contains(String.format(Locale.ROOT, "\"update_interval_in_days\":%d", datasource.getSchedule().getInterval())));
            assertTrue(json.contains(String.format(Locale.ROOT, "\"next_update_at_in_epoch_millis\"")));
            assertTrue(json.contains(String.format(Locale.ROOT, "\"provider\":\"%s\"", datasource.getDatabase().getProvider())));
            assertTrue(json.contains(String.format(Locale.ROOT, "\"sha256_hash\":\"%s\"", datasource.getDatabase().getSha256Hash())));
            assertTrue(
                json.contains(
                    String.format(Locale.ROOT, "\"updated_at_in_epoch_millis\":%d", datasource.getDatabase().getUpdatedAt().toEpochMilli())
                )
            );
            assertTrue(json.contains(String.format(Locale.ROOT, "\"valid_for_in_days\":%d", datasource.getDatabase().getValidForInDays())));
            for (String field : datasource.getDatabase().getFields()) {
                assertTrue(json.contains(field));
            }
            assertTrue(
                json.contains(
                    String.format(
                        Locale.ROOT,
                        "\"last_succeeded_at_in_epoch_millis\":%d",
                        datasource.getUpdateStats().getLastSucceededAt().toEpochMilli()
                    )
                )
            );
            assertTrue(
                json.contains(
                    String.format(
                        Locale.ROOT,
                        "\"last_processing_time_in_millis\":%d",
                        datasource.getUpdateStats().getLastProcessingTimeInMillis()
                    )
                )
            );
            assertTrue(
                json.contains(
                    String.format(
                        Locale.ROOT,
                        "\"last_failed_at_in_epoch_millis\":%d",
                        datasource.getUpdateStats().getLastFailedAt().toEpochMilli()
                    )
                )
            );
            assertTrue(
                json.contains(
                    String.format(
                        Locale.ROOT,
                        "\"last_skipped_at_in_epoch_millis\":%d",
                        datasource.getUpdateStats().getLastSkippedAt().toEpochMilli()
                    )
                )
            );

        }
    }

}
