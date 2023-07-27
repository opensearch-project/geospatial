/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.UnsupportedEncodingException;

import org.opensearch.OpenSearchException;
import org.opensearch.common.Strings;

public class InputFormatValidator {
    private static final int MAX_DATASOURCE_NAME_BYTES = 127;

    public String validateDatasourceName(final String datasourceName) {
        if (!Strings.validFileName(datasourceName)) {
            return "datasource name must not contain the following characters " + Strings.INVALID_FILENAME_CHARS;
        }
        if (datasourceName.isEmpty()) {
            return "datasource name must not be empty";
        }
        if (datasourceName.contains("#")) {
            return "datasource name must not contain '#'";
        }
        if (datasourceName.contains(":")) {
            return "datasource name must not contain ':'";
        }
        if (datasourceName.charAt(0) == '_' || datasourceName.charAt(0) == '-' || datasourceName.charAt(0) == '+') {
            return "datasource name must not start with '_', '-', or '+'";
        }
        int byteCount = 0;
        try {
            byteCount = datasourceName.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new OpenSearchException("unable to determine length of datasource name", e);
        }
        if (byteCount > MAX_DATASOURCE_NAME_BYTES) {
            return "datasource name is too long, (" + byteCount + " > " + MAX_DATASOURCE_NAME_BYTES + ")";
        }
        if (datasourceName.equals(".") || datasourceName.equals("..")) {
            return "datasource name must not be '.' or '..'";
        }
        return null;
    }
}
