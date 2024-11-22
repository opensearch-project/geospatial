/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for IpEnrichmentRequest.
 */
public class IpEnrichmentRequestTest {

    /**
     * Test validate() against a valid record.
     */
    @Test
    public void testValidateValidRequest() {
        IpEnrichmentRequest request = new IpEnrichmentRequest(
                "192.168.1.1", "ValidDataSourceName");
        Assertions.assertNull(request.validate());
    }

    /**
     * Test validate() against a valid record,
     * no error expected, because dataSourceName is optional.
     */
    @Test
    public void testValidateNullDataSourceName() {
        IpEnrichmentRequest request = new IpEnrichmentRequest(
                "192.168.1.1", null);
        Assertions.assertNull(request.validate());
    }

    /**
     * Test validate() against a valid record,
     * no error expected, because dataSourceName is optional.
     */
    @Test
    public void testValidateNullIpStringAndDataSourceName() {
        IpEnrichmentRequest request = new IpEnrichmentRequest(
                null, null);
        Assertions.assertEquals(1, request.validate().validationErrors().size());
    }


    /**
     * Test validate() against a valid record,
     * no error expected, because dataSourceName is optional.
     */
    @Test
    public void testFromActionRequestOnValidRecord() {
        String ipString = "192.168.1.1";
        String dsName = "demo";
        IpEnrichmentRequest request = new IpEnrichmentRequest(
                ipString, dsName);

        IpEnrichmentRequest requestAfterStream = IpEnrichmentRequest.fromActionRequest(request);

        Assertions.assertEquals(request.getIpString(), requestAfterStream.getIpString());
        Assertions.assertEquals(request.getDatasourceName(), requestAfterStream.getDatasourceName());
    }

}