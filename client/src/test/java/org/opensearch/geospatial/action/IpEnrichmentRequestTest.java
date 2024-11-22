/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IpEnrichmentRequestTest {


    // Test Validate
    // Success
    // Missing IP String
    // Missing DataSource is ok


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


    // Test StreamInput
        // Successful case
        // Junk input
        // Partial missing output


    // Test WriteTo
        // Write a valid record
        // Write with some junk data


    // Test FromActionRequest
        // Test valid record
        // Test non IPEnrichment request
        // Test with some junk.

}