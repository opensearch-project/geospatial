## Version 2.19.0.0 Release Notes

Compatible with OpenSearch 2.19.0

### Features
* Introduce new Java artifact geospatial-client to facilitate cross plugin communication. ([#700](https://github.com/opensearch-project/geospatial/pull/700))

### Infrastructure
* Github ci-runner Node.js issue fix ([#701](https://github.com/opensearch-project/geospatial/pull/701))
* Github CI pipeline update to publish geospatial-client Jar ([#706](https://github.com/opensearch-project/geospatial/pull/706))

### Refactoring
* Use instance of LockService instantiated in JobScheduler through Guice ([#677](https://github.com/opensearch-project/geospatial/pull/677))