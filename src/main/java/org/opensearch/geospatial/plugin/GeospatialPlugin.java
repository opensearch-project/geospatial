/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import java.util.Map;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;

/*

 */
public class GeospatialPlugin extends Plugin implements IngestPlugin {
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return MapBuilder.<String, Processor.Factory>newMapBuilder()
            .put(FeatureProcessor.TYPE, new FeatureProcessor.Factory())
            .immutableMap();
    }
}
