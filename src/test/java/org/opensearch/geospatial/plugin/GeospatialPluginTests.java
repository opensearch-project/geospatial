/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import java.util.Map;

import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.test.OpenSearchTestCase;

public class GeospatialPluginTests extends OpenSearchTestCase {

    public void testIsAnIngestPlugin() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        assertTrue(plugin instanceof IngestPlugin);
    }

    public void testFeatureProcessorIsAdded() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        Map<String, Processor.Factory> processors = plugin.getProcessors(null);
        assertTrue(processors.containsKey(FeatureProcessor.TYPE));
        assertTrue(processors.get(FeatureProcessor.TYPE) instanceof FeatureProcessor.Factory);
    }
}
