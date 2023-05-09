/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.shared;

import java.util.Locale;

import org.opensearch.Version;

public class Constants {
    public static final String USER_AGENT_KEY = "User-Agent";
    public static final String USER_AGENT_VALUE = String.format(Locale.ROOT, "OpenSearch/%s vanilla", Version.CURRENT.toString());
}
