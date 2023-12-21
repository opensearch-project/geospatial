/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static org.opensearch.geospatial.h3.H3.MAX_H3_RES;
import static org.opensearch.geospatial.h3.H3.MIN_H3_RES;
import static org.opensearch.geospatial.h3.H3.geoToH3;
import static org.opensearch.geospatial.h3.H3.h3IsValid;
import static org.opensearch.geospatial.h3.H3.h3ToLatLng;
import static org.opensearch.geospatial.h3.H3.stringToH3;

import java.util.Locale;

import org.opensearch.common.geo.GeoPoint;

import lombok.NonNull;

/**
 * Helper class for H3 library
 */
public class GeoHexHelper {

    /**
     * Checks whether given precision is within H3 Precision range
     * @param precision H3 index precision
     */
    public static void checkPrecisionRange(int precision) {
        if ((precision < MIN_H3_RES) || (precision > MAX_H3_RES)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid precision of %d . Must be between %d and %d.", precision, MIN_H3_RES, MAX_H3_RES)
            );
        }
    }

    /**
     * Converts from <code>long</code> representation of an index to {@link GeoPoint} representation.
     * @param h3CellID H3 Cell Id
     * @throws IllegalArgumentException if invalid h3CellID is provided
     */
    public static GeoPoint h3ToGeoPoint(long h3CellID) {
        if (h3IsValid(h3CellID) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid H3 Cell address: %d", h3CellID));
        }
        final var position = h3ToLatLng(h3CellID);
        return new GeoPoint(position.getLatDeg(), position.getLonDeg());
    }

    /**
     * Converts from {@link String} representation of an index to {@link GeoPoint} representation.
     * @param h3CellID H3 Cell Id
     * @throws IllegalArgumentException if invalid h3CellID is provided
     */
    public static GeoPoint h3ToGeoPoint(@NonNull String h3CellID) {
        return h3ToGeoPoint(stringToH3(h3CellID));
    }

    /**
     * Encodes longitude/latitude into H3 Cell Address for given precision
     *
     * @param latitude Latitude in degrees.
     * @param longitude Longitude in degrees.
     * @param precision Precision, 0 &lt;= res &lt;= 15
     * @return The H3 index.
     * @throws IllegalArgumentException latitude, longitude, or precision are out of range.
     */
    public static long longEncode(double longitude, double latitude, int precision) {
        return geoToH3(latitude, longitude, precision);
    }
}
