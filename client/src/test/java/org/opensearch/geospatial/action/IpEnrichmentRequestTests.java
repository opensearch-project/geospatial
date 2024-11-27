/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for IpEnrichmentRequest.
 */
public class IpEnrichmentRequestTests {

    /**
     * Test validate() against a valid record.
     */
    @Test
    public void testValidateValidRequest() {
        System.out.println("Test");
        IpEnrichmentRequest request = new IpEnrichmentRequest("192.168.1.1", "ValidDataSourceName");
        Assert.assertNull(request.validate());
    }

    /**
     * Test validate() against an invalid record,
     * Expecting an error being thrown as dataSource being null.
     */
    @Test
    public void testValidateNullDataSourceName() {
        IpEnrichmentRequest request = new IpEnrichmentRequest("192.168.1.1", null);
        Assert.assertEquals(1, request.validate().validationErrors().size());
    }


    /**
     * Test validate() against an invalid record,
     * Expecting an error being thrown as ipString being null.
     */
    @Test
    public void testValidateNullIpString() {
        IpEnrichmentRequest request = new IpEnrichmentRequest(null, "dataSource");
        Assert.assertEquals(1, request.validate().validationErrors().size());
    }


    /**
     * Test validate() against an invalid record,
     * Expecting an error with size in 2, because both fields are null.
     */
    @Test
    public void testValidateNullIpStringAndDataSourceName() {
        IpEnrichmentRequest request = new IpEnrichmentRequest(null, null);
        Assert.assertEquals(2, request.validate().validationErrors().size());
    }

    /**
     * Test fromActionRequest( ) to make sure the serialisation works.
     */
    @Test
    public void testFromActionRequestOnValidRecord() {
        String ipString = "192.168.1.1";
        String dsName = "demo";
        IpEnrichmentRequest request = new IpEnrichmentRequest(ipString, dsName);

        IpEnrichmentRequest requestAfterStream = IpEnrichmentRequest.fromActionRequest(request);

        Assert.assertEquals(request.getIpString(), requestAfterStream.getIpString());
        Assert.assertEquals(request.getDatasourceName(), requestAfterStream.getDatasourceName());
    }

}
