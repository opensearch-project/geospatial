## Version 2.4.0.0 Release Notes

Compatible with OpenSearch 2.4.0

### Features
* Support Uber's H3 geospatial indexing system as geohex_grid ([#179](https://github.com/opensearch-project/geospatial/pull/179))
* Add geojson support for XYPoint  ([#162](https://github.com/opensearch-project/geospatial/pull/162))
* Add XYPoint Field Type to index and query documents that contains cartesian points ([#130](https://github.com/opensearch-project/geospatial/pull/130))
* Add XYShapeQueryBuilder ([#82](https://github.com/opensearch-project/geospatial/pull/82))
* Add parameter to randomly include z coordinates to geometry ([#79](https://github.com/opensearch-project/geospatial/pull/79))
* Add shape processor ([#74](https://github.com/opensearch-project/geospatial/pull/74))
* Add shape field mapper ([#70](https://github.com/opensearch-project/geospatial/pull/70))
* Add ShapeIndexer to create indexable fields ([#68](https://github.com/opensearch-project/geospatial/pull/68))

### Enhancements
* add groupId to pluginzip publication ([#167](https://github.com/opensearch-project/geospatial/pull/167))
* Flip X and Y coordinates for WKT and array formats in XYPoint ([#156](https://github.com/opensearch-project/geospatial/pull/156))

### Infrastructure
* Add window and mac platform in CI ([#173](https://github.com/opensearch-project/geospatial/pull/173))
* Fix integration test failure with security enabled cluster ([#138](https://github.com/opensearch-project/geospatial/pull/138))
* Remove explicit dco check ([#126](https://github.com/opensearch-project/geospatial/pull/126))
* Include feature branch in workflow to trigger CI ([#102](https://github.com/opensearch-project/geospatial/pull/102))

### Maintenance
* Increment version to 2.4.0-SNAPSHOT ([#139](https://github.com/opensearch-project/geospatial/pull/139))
* Update to Gradle 7.5.1 ([#134](https://github.com/opensearch-project/geospatial/pull/134))

### Refactoring
* Remove optional to get features ([#177](https://github.com/opensearch-project/geospatial/pull/177))