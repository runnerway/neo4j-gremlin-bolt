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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Statement;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Rogelio J. Baucells
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@GraphFactoryClass(Neo4JGraphFactory.class)
public class Neo4JGraph implements Graph {

    private static class NoLabelReadPartition implements Neo4JReadPartition {

        @Override
        public boolean validateLabel(String label) {
            return true;
        }

        @Override
        public boolean containsVertex(Set<String> labels) {
            return true;
        }

        @Override
        public Set<String> vertexMatchPatternLabels() {
            return Collections.emptySet();
        }

        @Override
        public String vertexMatchPredicate(String alias) {
            return null;
        }
    }

    private class Neo4JTransaction extends AbstractThreadLocalTransaction {

        public Neo4JTransaction() {
            super(Neo4JGraph.this);
        }

        @Override
        protected void doOpen() {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // open database transaction
            session.beginTransaction();
        }

        @Override
        protected void doCommit() throws TransactionException {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // commit transaction
            session.commit();
        }

        @Override
        protected void doRollback() throws TransactionException {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // rollback transaction
            session.rollback();
        }

        @Override
        public boolean isOpen() {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // check transaction is open
            return session.isTransactionOpen();
        }

        @Override
        protected void doClose() {
            // close base
            super.doClose();
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // close transaction
            session.closeTransaction();
        }
    }

    private final Neo4JReadPartition partition;
    private final Set<String> vertexLabels;
    private final Driver driver;
    private final Neo4JElementIdProvider<?> vertexIdProvider;
    private final Neo4JElementIdProvider<?> edgeIdProvider;
    private final Neo4JElementIdProvider<?> propertyIdProvider;
    private final ThreadLocal<Neo4JSession> session = ThreadLocal.withInitial(() -> null);
    private final Neo4JTransaction transaction = new Neo4JTransaction();

    /**
     * Creates a {@link Neo4JGraph} instance.
     *
     * @param driver             The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider   The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider     The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     * @param propertyIdProvider The {@link Neo4JElementIdProvider} for the {@link org.apache.tinkerpop.gremlin.structure.VertexProperty} id generation.
     */
    public Neo4JGraph(Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Neo4JElementIdProvider<?> propertyIdProvider) {
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(propertyIdProvider, "propertyIdProvider cannot be null");
        // no label partition
        this.partition = new NoLabelReadPartition();
        this.vertexLabels = Collections.emptySet();
        // store driver instance
        this.driver = driver;
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        this.propertyIdProvider = propertyIdProvider;
    }

    /**
     * Creates a {@link Neo4JGraph} instance with the given partition within the neo4j database.
     *
     * @param partition          The {@link Neo4JReadPartition} within the neo4j database.
     * @param vertexLabels       The set of labels to append to vertices created by the {@link Neo4JGraph} session.
     * @param driver             The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider   The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider     The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     * @param propertyIdProvider The {@link Neo4JElementIdProvider} for the {@link org.apache.tinkerpop.gremlin.structure.VertexProperty} id generation.
     */
    public Neo4JGraph(Neo4JReadPartition partition, String[] vertexLabels, Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Neo4JElementIdProvider<?> propertyIdProvider) {
        Objects.requireNonNull(partition, "partition cannot be null");
        Objects.requireNonNull(vertexLabels, "vertexLabels cannot be null");
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(propertyIdProvider, "propertyIdProvider cannot be null");
        // initialize fields
        this.partition = partition;
        this.vertexLabels = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(vertexLabels)));
        this.driver = driver;
        // validate partition & additional labels
        if (!partition.containsVertex(this.vertexLabels))
            throw new IllegalArgumentException("Invalid vertexLabels, vertices created by the graph will not be part of the given partition");
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        this.propertyIdProvider = propertyIdProvider;
    }

    Neo4JSession currentSession() {
        // get current session
        Neo4JSession session = this.session.get();
        if (session == null) {
            // create new session
            session = new Neo4JSession(this, driver.session(), vertexIdProvider, edgeIdProvider, propertyIdProvider);
            // attach it to current thread
            this.session.set(session);
        }
        return session;
    }

    /**
     * Gets the {@link Neo4JReadPartition} that has been applied to current {@link Neo4JGraph}.
     *
     * @return The partition labels.
     */
    public Neo4JReadPartition getPartition() {
        return partition;
    }

    Set<String> vertexLabels() {
        return vertexLabels;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex addVertex(Object... keyValues) {
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // add vertex
        return session.addVertex(keyValues);
    }

    /**
     * Creates an index in the neo4j database.
     *
     * @param label        The label associated with the Index.
     * @param propertyName The property name associated with the Index.
     */
    public void createIndex(String label, String propertyName) {
        Objects.requireNonNull(label, "label cannot be null");
        Objects.requireNonNull(propertyName, "propertyName cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // execute statement
        session.executeStatement(new Statement("CREATE INDEX ON :`" + label + "`(" + propertyName + ")"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <C extends GraphComputer> C compute(Class<C> implementation) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(Object... ids) {
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.vertices(ids);
    }

    public Iterator<Vertex> vertices(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.vertices(statement)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Vertex> vertices(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.vertices(new Statement(statement))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Vertex> vertices(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.vertices(new Statement(statement, parameters))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Edge> edges(Object... ids) {
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find edges
        return session.edges(ids);
    }

    public Iterator<Edge> edges(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.edges(statement)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Edge> edges(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.edges(new Statement(statement))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Edge> edges(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.edges(new Statement(statement, parameters))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transaction tx() {
        // return transaction, do not open transaction here!
        return transaction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration configuration() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // get current session
        Neo4JSession session = this.session.get();
        if (session != null) {
            // close session
            session.close();
            // remove session
            this.session.remove();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StringFactory.graphString(this, "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Features features() {
        return new Neo4JGraphFeatures();
    }
}
