# neo4j-gremlin-bolt

This project allows the use of the [Apache Tinkerpop](http://tinkerpop.apache.org/) Java API with the [neo4j server](http://neo4j.com/) using the [BOLT](https://github.com/neo4j/neo4j-java-driver) protocol.

## Build status

[![Build Status](https://travis-ci.org/SteelBridgeLabs/neo4j-gremlin-bolt.svg?branch=master)](https://travis-ci.org/SteelBridgeLabs/neo4j-gremlin-bolt)
[![Coverage Status](https://coveralls.io/repos/github/SteelBridgeLabs/neo4j-gremlin-bolt/badge.svg?branch=master)](https://coveralls.io/github/SteelBridgeLabs/neo4j-gremlin-bolt?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.steelbridgelabs.oss/neo4j-gremlin-bolt/badge.svg?style=flat-square)](https://maven-badges.herokuapp.com/maven-central/com.steelbridgelabs.oss/neo4j-gremlin-bolt/)

## Requirements

* Java 8.
* Maven 3.0.0 or newer.

## Usage

Add the Neo4j [Apache Tinkerpop](http://tinkerpop.apache.org/) implementation to your project:

### Maven

```xml
    <dependency>
        <groupId>com.steelbridgelabs.oss</groupId>
        <artifactId>neo4j-gremlin-bolt</artifactId>
        <version>{version}</version>
    </dependency>
```

*Please check the [Maven Central](https://maven-badges.herokuapp.com/maven-central/com.steelbridgelabs.oss/neo4j-gremlin-bolt/) for the latest version available.

## License

neo4j-gremlin-bolt and it's modules are licensed under the [Apache License v 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Features

* [Apache Tinkerpop](http://tinkerpop.apache.org/) 3.x Online Transactional Processing Graph Systems (OLTP) support.
* [neo4j](http://neo4j.com/) implementation on top of the [BOLT](https://github.com/neo4j/neo4j-java-driver) protocol.
* Support for [Graph partitioning](https://github.com/SteelBridgeLabs/neo4j-gremlin-bolt/blob/master/src/main/java/com/steelbridgelabs/oss/neo4j/structure/Neo4JReadPartition.java), out of the box implementation for All labels and Any label partitions.

# Graph API

## Element ID providers

The library supports an open architecture for element ID generation for new Vertices and Edges. Two element ID providers are provided out of the box:

* Neo4J native id() support, see [Neo4JNativeElementIdProvider](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/providers/Neo4JNativeElementIdProvider.html) for more information.

```java
    // create id provider
    Neo4JElementIdProvider<?> provider = new Neo4JNativeElementIdProvider();
```

Advantage:

 * Fewer database Hits on MATCH statements.

* Database sequence support, see [Neo4JNativeElementIdProvider](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/providers/Neo4JNativeElementIdProvider.html) for more information.

```java
    // create id provider
    Neo4JElementIdProvider<?> provider = new DatabaseSequenceElementIdProvider(driver);
```

## Connecting to the database

* Create driver instance, see [neo4j-java-driver](https://github.com/neo4j/neo4j-java-driver) for more information.

```java
    // create driver instance
    Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "neo4j"));
```

* Create element id provider instances, see [providers](#element-id-providers) for more information. 

```java
    // create id provider instances
    vertexIdProvider = ...
    edgeIdProvider = ...
```

* Create [Graph](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Graph.html) instance.

```java
    // create graph instance
    try (Graph graph = new Neo4JGraph(driver, vertexIdProvider, edgeIdProvider)) {
        
    }
```

## Working with transactions

* Obtain a [Transaction](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Transaction.html) instance from current Graph.

```java
    // create graph instance
    try (Graph graph = new Neo4JGraph(driver, vertexIdProvider, edgeIdProvider)) {
        // begin transaction
        try (Transaction transaction = graph.tx()) {
            // use Graph API to create, update and delete Vertices and Edges
            
            // commit transaction
            transaction.commit();
        }
    }
```

## Enabling Neo4J profiler

* Set logger INFO level to the package: com.steelbridgelabs.oss.neo4j.structure.summary 

* Enable profiler to the [Graph](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Graph.html) instance.

```java
    // create graph instance
    try (Neo4JGraph graph = new Neo4JGraph(driver, vertexIdProvider, edgeIdProvider)) {
        // enable profiler
        graph.setProfilerEnabled(true);
        
    }
```

The library will prefix CYPHER statements with the PROFILE clause dumping the output into the log file, example: 

````
2016-08-26 23:19:42.226  INFO 98760 --- [-f6753a03391b-1] c.s.o.n.s.summary.ResultSummaryLogger    : Profile for CYPHER statement: Statement{text='PROFILE MATCH (n:Person{id: {id}})-[r:HAS_ADDRESS]->(m) RETURN n, r, m', parameters={id: 1306984}}

+----------------------+----------------+------+---------+-----------+
| Operator             + Estimated Rows + Rows + DB Hits + Variables |
+----------------------+----------------+------+---------+-----------+
| +ProduceResults      |              0 |    1 |       0 | m, n, r   |
| |                    +----------------+------+---------+-----------+
| +Expand(All)         |              0 |    1 |       2 | m, n, r   |
| |                    +----------------+------+---------+-----------+
| +Filter              |              0 |    1 |       1 | n         |
| |                    +----------------+------+---------+-----------+
| +NodeUniqueIndexSeek |              0 |    1 |       2 | n         |
+----------------------+----------------+------+---------+-----------+
````

## Working with Vertices and Edges

### Create a Vertex

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` call the [Graph.addVertex()](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Graph.html#addVertex-java.lang.Object...-) method.

```java
  // create a vertex in current graph
  Vertex vertex = graph.addVertex();
```

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` with property values: 

```java
  // create a vertex in current graph with property values
  Vertex vertex = graph.addVertex("name", "John", "age", 50);
```

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` with a Label: 

```java
  // create a vertex in current graph with label
  Vertex vertex1 = graph.addVertex("Person");
  // create another vertex in current graph with label
  Vertex vertex2 = graph.addVertex(T.label, "Company");
```
