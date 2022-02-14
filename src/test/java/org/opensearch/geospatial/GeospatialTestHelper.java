/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial;

import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.FIELD_DATA;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.common.Randomness;
import org.opensearch.geospatial.action.upload.geojson.ContentBuilder;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent;
import org.opensearch.test.OpenSearchTestCase;

public class GeospatialTestHelper {
    public static final int RANDOM_STRING_MIN_LENGTH = 2;
    public static final int RANDOM_STRING_MAX_LENGTH = 16;

    public static Map<String, Object> buildRequestContent(int featureCount) {
        JSONObject contents = new JSONObject();
        if (Randomness.get().nextBoolean()) {
            contents.put(ContentBuilder.GEOJSON_FEATURE_ID_FIELD, randomLowerCaseString());
        }
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), randomLowerCaseString());
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), randomLowerCaseString());
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName(), "geo_shape");
        JSONArray values = new JSONArray();
        IntStream.range(0, featureCount).forEach(notUsed -> { values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap()))); });
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents.toMap();
    }

    private static String randomString() {
        return OpenSearchTestCase.randomAlphaOfLengthBetween(RANDOM_STRING_MIN_LENGTH, RANDOM_STRING_MAX_LENGTH);
    }

    public static String randomLowerCaseString() {
        return randomString().toLowerCase(Locale.getDefault());
    }

}
