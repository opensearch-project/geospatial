/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.test.OpenSearchTestCase.randomBoolean;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.opensearch.geospatial.GeospatialTestHelper;

public class UploadStatsBuilder {
    private static final int MAX_METRIC_COUNT = 10;
    private static final int MIN_METRIC_COUNT = 2;

    public static UploadStats randomUploadStats() {
        int randomMetricCount = randomIntBetween(MIN_METRIC_COUNT, MAX_METRIC_COUNT);
        UploadStats stats = new UploadStats();
        IntStream.range(0, randomMetricCount).forEach(unUsed -> {
            stats.addMetric(GeospatialTestHelper.generateRandomUploadMetric());
            stats.incrementAPICount();
        });
        // simulate failed upload by randomly increasing the api count
        if (randomBoolean()) {
            stats.incrementAPICount();
        }
        return stats;
    }

    public static List<UploadStats> randomUploadStats(int max) {
        List<UploadStats> stats = new ArrayList<>();
        IntStream.range(0, max).forEach(unUsed -> stats.add(randomUploadStats()));
        return stats;
    }
}
