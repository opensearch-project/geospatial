/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.opensearch.common.Randomness;
import org.opensearch.common.Strings;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class InputFormatValidatorTests extends Ip2GeoTestCase {
    public void testValidateDatasourceName_whenValidName_thenSucceed() {
        ParameterValidator inputFormatValidator = new ParameterValidator();
        String validDatasourceName = GeospatialTestHelper.randomLowerCaseString();

        // Run
        List<String> errorMsgs = inputFormatValidator.validateDatasourceName(validDatasourceName);

        // Verify
        assertTrue(errorMsgs.isEmpty());
    }

    public void testValidate_whenInvalidDatasourceNames_thenFails() {
        ParameterValidator inputFormatValidator = new ParameterValidator();
        String validDatasourceName = GeospatialTestHelper.randomLowerCaseString();
        String fileNameChar = validDatasourceName + Strings.INVALID_FILENAME_CHARS.stream()
            .skip(Randomness.get().nextInt(Strings.INVALID_FILENAME_CHARS.size() - 1))
            .findFirst();
        String startsWith = Arrays.asList("_", "-", "+").get(Randomness.get().nextInt(3)) + validDatasourceName;
        String empty = "";
        String hash = validDatasourceName + "#";
        String colon = validDatasourceName + ":";
        StringBuilder longName = new StringBuilder();
        while (longName.length() <= 127) {
            longName.append(GeospatialTestHelper.randomLowerCaseString());
        }
        String point = Arrays.asList(".", "..").get(Randomness.get().nextInt(2));
        Map<String, String> nameToError = Map.of(
            fileNameChar,
            "not contain the following characters",
            empty,
            "must not be empty",
            hash,
            "must not contain '#'",
            colon,
            "must not contain ':'",
            startsWith,
            "must not start with",
            longName.toString(),
            "name is too long",
            point,
            "must not be '.' or '..'"
        );

        for (Map.Entry<String, String> entry : nameToError.entrySet()) {

            // Run
            List<String> errorMsgs = inputFormatValidator.validateDatasourceName(entry.getKey());

            // Verify
            assertFalse(errorMsgs.isEmpty());
            assertTrue(errorMsgs.get(0).contains(entry.getValue()));
        }
    }
}
