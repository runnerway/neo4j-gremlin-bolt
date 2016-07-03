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

import java.util.Set;

/**
 * @author Rogelio J. Baucells
 */
public interface Neo4JReadPartition {

    /**
     * Checks if the partition has the given label.
     *
     * @param label The label to check in the partition.
     * @return <code>true</code> if the label is in the partition, otherwise <code>false</code>.
     */
    boolean containsLabel(String label);

    /**
     * Checks if the partition has the given vertex (labels in vertex).
     *
     * @param labels The label to check in the partition.
     * @return <code>true</code> if the vertex is in the partition, otherwise <code>false</code>.
     */
    boolean containsVertex(Set<String> labels);

    /**
     * Gets the set of labels required at the time of matching the vertex in a Cypher MATCH pattern.
     *
     * @return The set of labels.
     */
    Set<String> vertexMatchPatternLabels();

    /**
     * Generates a {@link org.apache.tinkerpop.gremlin.structure.Vertex} Cypher MATCH predicate, example:
     * <p>
     * (alias:Label1 OR alias:Label2)
     * </p>
     *
     * @param alias The vertex alias in the MATCH Cypher statement.
     * @return The Cypher MATCH predicate if required by the vertex, otherwise <code>null</code>.
     */
    String vertexMatchPredicate(String alias);
}
