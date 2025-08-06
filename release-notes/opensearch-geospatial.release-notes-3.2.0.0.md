## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Maintenance
* Upgrade gradle to 8.14.3 and run CI checks with JDK24 ([#776](https://github.com/opensearch-project/geospatial/pull/776))

### Refactoring
* Replace usages of ThreadContext.stashContext with pluginSubject.runAs ([#715](https://github.com/opensearch-project/geospatial/pull/715))