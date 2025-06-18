## Version 3.1.0.0 Release Notes

Compatible with OpenSearch 3.1.0

### Bug Fixes
* Reset datasource metadata when failed to update it in postIndex and postDelete to force refresh it from the primary index shard. ([#761](https://github.com/opensearch-project/geospatial/pull/761))

### Maintenance
* Fix a unit test and update github workflow to use actions/setup-java@v3.