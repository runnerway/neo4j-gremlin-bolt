/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.steelbridgelabs.oss.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JVertexWhileGettingOutEdgesTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock
    private Neo4JSession session;

    @Mock
    private Neo4JReadPartition partition;

    @Mock
    private Node node;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private Graph.Features.VertexFeatures vertexFeatures;

    @Mock
    private Graph.Features features;

    @Mock
    private Neo4JVertex otherVertex;

    @Mock
    private Neo4JEdge edge1;

    @Mock
    private Neo4JEdge edge2;

    @Test
    public void givenNoLabelsShouldGetEdges() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`l1`{id: {id}})-[r]->(m) return n, r, m", Collections.singletonMap("id", 1L))))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenNoLabelsShouldGetDatabaseAndTransientEdges() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`l1`{id: {id}})-[r]->(m) WHERE NOT r.id IN {ids} return n, r, m", parameters)))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertTrue("Edges iterator does not contain two elements", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenLabelShouldGetDatabaseAndTransientEdges() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL");
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`l1`{id: {id}})-[r:`EL`]->(m) WHERE NOT r.id IN {ids} return n, r, m", parameters)))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertTrue("Edges iterator does not contain two elements", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenLabelsShouldGetDatabaseAndTransientEdges() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`l1`{id: {id}})-[r:`EL2`|:`EL1`]->(m) WHERE NOT r.id IN {ids} return n, r, m", parameters)))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL1", "EL2");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertTrue("Edges iterator does not contain two elements", edges.hasNext());
    }

    @Test
    public void givenLabelShouldGetDatabaseEdges() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`l1`{id: {id}})-[r:`EL1`]->(m) WHERE NOT r.id IN {ids} return n, r, m", parameters)))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL1");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertFalse("Edges iterator cannot not contain two elements", edges.hasNext());
    }

    @Test
    public void givenLabelShouldGetTransientEdges() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`l1`{id: {id}})-[r:`EL2`]->(m) WHERE NOT r.id IN {ids} return n, r, m", parameters)))).thenAnswer(invocation -> Stream.empty());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL2");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertFalse("Edges iterator cannot not contain two elements", edges.hasNext());
    }

    @Test
    public void givenNoLabelsAndPartitionMatchPatternShouldGetEdges() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.vertexMatchPatternLabels()).thenAnswer(invocation -> new HashSet<>(Arrays.asList("P1", "P2")));
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Arrays.asList("l1", "P1", "P2"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 2L);
        Mockito.when(session.edges(Mockito.eq(new Statement("MATCH (n:`P1`:`P2`:`l1`{id: {id}})-[r]->(m:`P1`:`P2`) return n, r, m", Collections.singletonMap("id", 1L))))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, "id", node);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }
}
