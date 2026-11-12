## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Bug Fixes

* Fix ip2geo datasource refresh failure after snapshot restore by setting refresh policy on the BulkRequest instead of individual item requests ([#909](https://github.com/opensearch-project/geospatial/pull/909))
