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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Session;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JSessionWhileAddVertexTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private Graph.Features.VertexFeatures vertexFeatures;

    @Mock
    private Graph.Features features;

    @Mock
    private Neo4JReadPartition partition;

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVertexWithDefaultLabel() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Vertex vertex = session.addVertex();
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
        }
    }

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVertexWithId() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Vertex vertex = session.addVertex();
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex id", 1L, vertex.id());
        }
    }

    @Test
    public void givenLabelShouldCreateVertex() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Vertex vertex = session.addVertex(T.label, "label1");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", "label1", vertex.label());
        }
    }

    @Test
    public void givenLabelsShouldCreateVertex() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Neo4JVertex vertex = session.addVertex(T.label, "label1::label2::label3");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", "label1::label2::label3", vertex.label());
            Assert.assertArrayEquals("Failed to assign vertex labels", new String[]{"label1", "label2", "label3"}, vertex.labels());
        }
    }

    @Test
    public void givenKeyValuePairsShouldCreateVertexWithProperties() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Neo4JVertex vertex = session.addVertex("k1", "v1", "k2", 2L, "k3", true);
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k1"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k1").value(), "v1");
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k2"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k2").value(), 2L);
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k3"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k3").value(), true);
        }
    }
}
