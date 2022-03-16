- [Developer Guide](#developer-guide)
  - [Getting Started](#getting-started)
    - [Fork OpenSearch geospatial Repo](#fork-opensearch-geospatial-repo)
    - [Install Prerequisites](#install-prerequisites)
      - [JDK 11](#jdk-11)
  - [Use an Editor](#use-an-editor)
    - [IntelliJ IDEA](#intellij-idea)
  - [Build](#build)
    - [Run Single-node Cluster Locally](#run-single-node-cluster-locally)
  - [Submitting Changes](#submitting-changes)

# Developer Guide

So you want to contribute code to OpenSearch geospatial? Excellent! We're glad you're here. Here's what you need to do.

## Getting Started

### Fork OpenSearch geospatial Repo

Fork [opensearch-project/geospatial](https://github.com/opensearch-project/geospatial) and clone locally.

Example:
```
git clone https://github.com/[your username]/geospatial.git
```

### Install Prerequisites

#### JDK 11

OpenSearch builds using Java 11 at a minimum. This means you must have a JDK 11 installed with the environment variable 
`JAVA_HOME` referencing the path to Java home for your JDK 11 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-11`.

One easy way to get Java 11 on *nix is to use [sdkman](https://sdkman.io/).

```bash
curl -s "https://get.sdkman.io" | bash
source ~/.sdkman/bin/sdkman-init.sh
sdk install java 11.0.2-open
sdk use java 11.0.2-open
```

Team has to replace minimum JDK version 14 as it was not an LTS release. JDK 14 should still work for most scenarios.

## Use an Editor

### IntelliJ IDEA

When importing into IntelliJ you will need to define an appropriate JDK. The convention is that **this SDK should be named "11"**, and the project import will detect it automatically. For more details on defining an SDK in IntelliJ please refer to [this documentation](https://www.jetbrains.com/help/idea/sdk.html#define-sdk). Note that SDK definitions are global, so you can add the JDK from any project, or after project import. Importing with a missing JDK will still work, IntelliJ will report a problem and will refuse to build until resolved.

You can import the OpenSearch project into IntelliJ IDEA as follows.

1. Select **File > Open**
2. In the subsequent dialog navigate to the root `build.gradle` file
3. In the subsequent dialog select **Open as Project**

## Java Language Formatting Guidelines

Taken from [OpenSearch's guidelines](https://github.com/opensearch-project/OpenSearch/blob/main/DEVELOPER_GUIDE.md):

Java files in the OpenSearch codebase are formatted with the Eclipse JDT formatter, using the [Spotless Gradle](https://github.com/diffplug/spotless/tree/master/plugin-gradle) plugin. The formatting check can be run explicitly with:

    ./gradlew spotlessJavaCheck

The code can be formatted with:

    ./gradlew spotlessApply

Please follow these formatting guidelines:

* Java indent is 4 spaces
* Line width is 140 characters
* Lines of code surrounded by `// tag::NAME` and `// end::NAME` comments are included in the documentation and should only be 76 characters wide not counting leading indentation. Such regions of code are not formatted automatically as it is not possible to change the line length rule of the formatter for part of a file. Please format such sections sympathetically with the rest of the code, while keeping lines to maximum length of 76 characters.
* Wildcard imports (`import foo.bar.baz.*`) are forbidden and will cause the build to fail.
* If *absolutely* necessary, you can disable formatting for regions of code with the `// tag::NAME` and `// end::NAME` directives, but note that these are intended for use in documentation, so please make it clear what you have done, and only do this where the benefit clearly outweighs the decrease in consistency.
* Note that JavaDoc and block comments i.e. `/* ... */` are not formatted, but line comments i.e `// ...` are.
* There is an implicit rule that negative boolean expressions should use the form `foo == false` instead of `!foo` for better readability of the code. While this isn't strictly enforced, if might get called out in PR reviews as something to change.

In order to gradually introduce the spotless formatting, we use the 
[ratchetFrom](https://github.com/diffplug/spotless/tree/main/plugin-gradle#ratchet) spotless functionality. This makes 
it so only files that are changed compared to the origin branch are inspected. Because of this, ensure that your 
origin branch is up to date with the plugins upstream when testing locally.

## Build

OpenSearch geospatial uses a [Gradle](https://docs.gradle.org/6.6.1/userguide/userguide.html) wrapper for its build. 
Run `gradlew` on Unix systems.

Build OpenSearch geospatial using `gradlew build` 

```
./gradlew build
```

## Run OpenSearch geospatial

### Run Single-node Cluster Locally
Run OpenSearch geospatial using `gradlew run`.

```shell script
./gradlew run
```
That will build OpenSearch and start it, writing its log above Gradle's status message. We log a lot of stuff on startup, specifically these lines tell you that plugin is ready.
```
[2020-05-29T14:50:35,167][INFO ][o.e.h.AbstractHttpServerTransport] [runTask-0] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
[2020-05-29T14:50:35,169][INFO ][o.e.n.Node               ] [runTask-0] started
```

It's typically easier to wait until the console stops scrolling, and then run `curl` in another window to check if OpenSearch instance is running.

```bash
curl localhost:9200

{
  "name" : "integTest-0",
  "cluster_name" : "integTest",
  "cluster_uuid" : "zfrxOoUXT2GlY4w4SlJ5gQ",
  "version" : {
    "distribution" : "opensearch",
    "number" : "1.3.0-SNAPSHOT",
    "build_type" : "tar",
    "build_hash" : "93bd32b14270be0da8a6b5eef8eeabfce7eb2b58",
    "build_date" : "2021-12-06T00:09:53.879242Z",
    "build_snapshot" : true,
    "lucene_version" : "8.10.1",
    "minimum_wire_compatibility_version" : "6.8.0",
    "minimum_index_compatibility_version" : "6.0.0-beta1"
  },
  "tagline" : "The OpenSearch Project: https://opensearch.org/"
}

```

## Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).
