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

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Session;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JSessionWhileAddEdgeTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Neo4JVertex outVertex;

    @Mock
    private Neo4JVertex inVertex;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private Neo4JReadPartition partition;

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVEdgeWithLabel() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> Mockito.mock(Transaction.class));
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Neo4JEdge edge = session.addEdge("label1", outVertex, inVertex);
            // assert
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertEquals("Failed to assign edge label", "label1", edge.label());
        }
    }

    @Test
    public void givenEmptyKeyValuePairsShouldCreateEdgeWithId() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> Mockito.mock(Transaction.class));
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Neo4JEdge edge = session.addEdge("label1", outVertex, inVertex);
            // assert
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertEquals("Failed to assign edge id", 1L, edge.id());
        }
    }

    @Test
    public void givenKeyValuePairsShouldCreateEdgeWithProperties() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> Mockito.mock(Transaction.class));
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.idFieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generateId()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, provider)) {
            // act
            Neo4JEdge edge = session.addEdge("label1", outVertex, inVertex, "k1", "v1", "k2", 2L, "k3", true);
            // assert
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertNotNull("Failed to assign edge property", edge.property("k1"));
            Assert.assertEquals("Failed to assign edge label", edge.property("k1").value(), "v1");
            Assert.assertNotNull("Failed to assign edge property", edge.property("k2"));
            Assert.assertEquals("Failed to assign edge label", edge.property("k2").value(), 2L);
            Assert.assertNotNull("Failed to assign edge property", edge.property("k3"));
            Assert.assertEquals("Failed to assign edge label", edge.property("k3").value(), true);
        }
    }
}
