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

package com.steelbridgelabs.oss.neo4j.structure.partitions;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JReadPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link Neo4JReadPartition} implementation creates a {@link org.apache.tinkerpop.gremlin.structure.Graph} partition
 * where all {@link org.apache.tinkerpop.gremlin.structure.Vertex} in graph contain at least one of the partition labels.
 *
 * @author Rogelio J. Baucells
 */
public class AnyLabelReadPartition implements Neo4JReadPartition {

    private final Set<String> labels;

    public AnyLabelReadPartition(String... labels) {
        Objects.requireNonNull(labels, "labels cannot be null");
        // store labels
        this.labels = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(labels)));
    }

    /**
     * Checks if the partition has the given label.
     *
     * @param label The label to check in the partition.
     * @return <code>true</code> if the label is in the partition, otherwise <code>false</code>.
     */
    @Override
    public boolean containsLabel(String label) {
        Objects.requireNonNull(label, "label cannot be null");
        // check label is in set
        return labels.contains(label);
    }

    /**
     * Checks if the partition has the given vertex (labels in vertex). This implementation enforces at least one
     * of the labels in partition is present in vertex.
     *
     * @param labels The label to check in the partition.
     * @return <code>true</code> if the vertex is in the partition, otherwise <code>false</code>.
     */
    @Override
    public boolean containsVertex(Set<String> labels) {
        Objects.requireNonNull(labels, "labels cannot be null");
        // at least one label must be present in vertex
        return this.labels.stream().anyMatch(labels::contains);
    }

    /**
     * Gets the set of labels required at the time of matching the vertex in a Cypher MATCH pattern. This implementation
     * returns a single label if partition contains a single label, otherwise an empty set (predicate required to match vertices).
     *
     * @return The set of labels.
     */
    @Override
    public Set<String> vertexMatchPatternLabels() {
        // use match pattern if only one label in partition
        return labels.size() == 1 ? labels : Collections.emptySet();
    }

    /**
     * Generates a {@link org.apache.tinkerpop.gremlin.structure.Vertex} Cypher MATCH predicate, example:
     * <p>
     * (alias:Label1 OR alias:Label2)
     * </p>
     *
     * @param alias The vertex alias in the MATCH Cypher statement.
     * @return The Cypher MATCH predicate if required by the vertex, otherwise <code>null</code>.
     */
    @Override
    public String vertexMatchPredicate(String alias) {
        Objects.requireNonNull(alias, "alias cannot be null");
        // check we require predicate, if only one label in partition we use match pattern
        if (labels.size() != 1) {
            // match predicate where at least one vertex label must be present in partition (alias:`label1` OR alias:`label2` ... alias.`labeln`)
            return labels.stream().map(label -> alias + ":`" + label + "`").collect(Collectors.joining(" OR ", "(", ")"));
        }
        return null;
    }
}
