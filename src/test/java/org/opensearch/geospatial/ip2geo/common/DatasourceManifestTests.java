/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLConnection;

import lombok.SneakyThrows;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.shared.Constants;

@SuppressForbidden(reason = "unit test")
public class DatasourceManifestTests extends Ip2GeoTestCase {

    @SneakyThrows
    public void testInternalBuild_whenCalled_thenCorrectUserAgentValueIsSet() {
        URLConnection connection = mock(URLConnection.class);
        File manifestFile = new File(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").getFile());
        when(connection.getInputStream()).thenReturn(new FileInputStream(manifestFile));

        // Run
        DatasourceManifest manifest = DatasourceManifest.Builder.internalBuild(connection);

        // Verify
        verify(connection).addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
        assertEquals("https://test.com/db.zip", manifest.getUrl());
    }
}
