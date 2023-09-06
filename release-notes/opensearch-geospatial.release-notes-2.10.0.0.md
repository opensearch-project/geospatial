## Version 2.10.0.0 Release Notes

Compatible with OpenSearch 2.10.0

### Features
* IP2Geo processor implementation ([#362](https://github.com/opensearch-project/geospatial/pull/362))

### Bug Fixes
* Revert datasource state when delete fails([#382](https://github.com/opensearch-project/geospatial/pull/382))
* Update ip2geo test data url([#389](https://github.com/opensearch-project/geospatial/pull/389))

### Infrastructure
* Make jacoco report to be generated faster in local ([#267](https://github.com/opensearch-project/geospatial/pull/267))
* Exclude lombok generated code from jacoco coverage report ([#268](https://github.com/opensearch-project/geospatial/pull/268))

### Maintenance
* Change package for Strings.hasText ([#314](https://github.com/opensearch-project/geospatial/pull/314))
* Fixed compilation errors after refactoring in core foundation classes ([#380](https://github.com/opensearch-project/geospatial/pull/380))
* Version bump for spotlss and apache commons([#400](https://github.com/opensearch-project/geospatial/pull/400))
### Refactoring
* Refactor LifecycleComponent package path ([#377](https://github.com/opensearch-project/geospatial/pull/377))
* Refactor Strings utility methods to core library ([#379](https://github.com/opensearch-project/geospatial/pull/379))
