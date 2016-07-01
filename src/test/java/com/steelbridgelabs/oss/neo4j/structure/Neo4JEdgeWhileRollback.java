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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Collections;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JEdgeWhileRollback {

    @Test
    public void givenStringPropertyShouldRollbackToOriginalValue() {
        // arrange
        Neo4JGraph graph = Mockito.mock(Neo4JGraph.class);
        Mockito.when(graph.tx()).thenAnswer(invocation -> Mockito.mock(Transaction.class));
        Neo4JSession session = Mockito.mock(Neo4JSession.class);
        Neo4JVertex out = Mockito.mock(Neo4JVertex.class);
        Neo4JVertex in = Mockito.mock(Neo4JVertex.class);
        Relationship relationship = Mockito.mock(Relationship.class);
        Mockito.when(relationship.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(relationship.type()).thenAnswer(invocation -> "label");
        Mockito.when(relationship.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(relationship.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Neo4JEdge edge = new Neo4JEdge(graph, session, "id", out, relationship, in);
        edge.property("key1", "value2");
        // act
        edge.rollback();
        // assert
        Assert.assertNotNull(edge.property("key1"));
        Property<String> property = edge.property("key1");
        Assert.assertEquals("Failed to rollback property value", "value1", property.value());
    }

    @Test
    public void givenDirtyEdgeShouldRollbackToOriginalState() {
        // arrange
        Neo4JGraph graph = Mockito.mock(Neo4JGraph.class);
        Mockito.when(graph.tx()).thenAnswer(invocation -> Mockito.mock(Transaction.class));
        Neo4JSession session = Mockito.mock(Neo4JSession.class);
        Neo4JVertex out = Mockito.mock(Neo4JVertex.class);
        Neo4JVertex in = Mockito.mock(Neo4JVertex.class);
        Relationship relationship = Mockito.mock(Relationship.class);
        Mockito.when(relationship.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(relationship.type()).thenAnswer(invocation -> "label");
        Mockito.when(relationship.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(relationship.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Neo4JEdge edge = new Neo4JEdge(graph, session, "id", out, relationship, in);
        edge.property("key1", "value2");
        // act
        edge.rollback();
        // assert
        Assert.assertFalse("Failed to rollback edge state", edge.isDirty());
    }
}
