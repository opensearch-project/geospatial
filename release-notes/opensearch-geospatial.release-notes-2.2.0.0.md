## Version 2.2.0.0 Release Notes

Compatible with OpenSearch 2.2.0

### Features

* Add feature processor to convert geo-json feature to geo-shape field ([#15](https://github.com/opensearch-project/geospatial/pull/15))
* Add rest handler for geo-json upload ([#25](https://github.com/opensearch-project/geospatial/pull/25))
* Create UploadGeoJSONRequest content as an object ([#32](https://github.com/opensearch-project/geospatial/pull/32))
* Add GeoJSON object of type FeatureCollection ([#33](https://github.com/opensearch-project/geospatial/pull/33))
* Include new route to support update index while upload ([#34](https://github.com/opensearch-project/geospatial/pull/34))
* Add uploader to upload user input ([#35](https://github.com/opensearch-project/geospatial/pull/35))
* Make field name as optional ([#37](https://github.com/opensearch-project/geospatial/pull/37))
* Use BulkResponse build error message ([#46](https://github.com/opensearch-project/geospatial/pull/46))
* Update upload API response structure ([#51](https://github.com/opensearch-project/geospatial/pull/51))
* Add metric and stat entity ([#54](https://github.com/opensearch-project/geospatial/pull/54))
* Create Upload Stats Service to build response for stats API ([#62](https://github.com/opensearch-project/geospatial/pull/62))
* Include stats api to provide upload metrics ([#64](https://github.com/opensearch-project/geospatial/pull/64))

### Infrastructure
* Create plugin using plugin template ([#3](https://github.com/opensearch-project/geospatial/pull/3))
* Add formatter config from OpenSearch ([#21](https://github.com/opensearch-project/geospatial/pull/21))
* Adding JDK 11 to CI matrix ([#31](https://github.com/opensearch-project/geospatial/pull/31))
* Add support to run integration tests with multiple nodes ([#57](https://github.com/opensearch-project/geospatial/pull/57))

### Maintenance
* Update OpenSearch upstream version to 2.2.0([#87](https://github.com/opensearch-project/geospatial/pull/87))