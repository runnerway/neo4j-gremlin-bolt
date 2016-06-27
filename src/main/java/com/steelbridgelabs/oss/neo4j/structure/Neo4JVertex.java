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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Rogelio J. Baucells
 */
public class Neo4JVertex extends Neo4JElement implements Vertex {

    private static class Neo4JVertexProperty<T> implements VertexProperty<T> {

        private final Neo4JVertex vertex;
        private final Object id;
        private final String name;
        private final T value;

        public Neo4JVertexProperty(Neo4JVertex vertex, Object id, String name, T value) {
            Objects.requireNonNull(vertex, "vertex cannot be null");
            Objects.requireNonNull(id, "id cannot be null");
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(value, "value cannot be null");
            // store fields
            this.vertex = vertex;
            this.id = id;
            this.name = name;
            this.value = value;
        }

        @Override
        public Vertex element() {
            return vertex;
        }

        @Override
        public <U> Iterator<Property<U>> properties(String... propertyKeys) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        @Override
        public Object id() {
            return id;
        }

        @Override
        public <V> Property<V> property(String key, V value) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        @Override
        public String key() {
            return name;
        }

        @Override
        public T value() throws NoSuchElementException {
            return value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public void remove() {
            // check cardinality
            Cardinality cardinality = vertex.cardinalities.get(name);
            if (cardinality != null) {
                // check it is single value
                if (cardinality != Cardinality.single) {
                    // get list of properties in vertex
                    Collection<?> vertexProperties = vertex.properties.get(name);
                    if (vertexProperties != null) {
                        // remove this instance from list
                        vertexProperties.remove(this);
                        // check properties are empty, remove key from vertex properties
                        if (vertexProperties.isEmpty()) {
                            // remove property
                            vertex.properties.remove(name);
                            // remove cardinality
                            vertex.cardinalities.remove(name);
                        }
                    }
                }
                else {
                    // remove property
                    vertex.properties.remove(name);
                    // remove cardinality
                    vertex.cardinalities.remove(name);
                }
            }
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof VertexProperty && ElementHelper.areEqual(this, object);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode((Element)this);
        }

        @Override
        public String toString() {
            return StringFactory.propertyString(this);
        }
    }

    private final Graph graph;
    private final Neo4JSession session;
    private final Neo4JElementIdProvider propertyIdProvider;
    private final Map<String, Collection<VertexProperty>> properties = new HashMap<>();
    private final Map<String, VertexProperty.Cardinality> cardinalities = new HashMap<>();
    private final Set<Neo4JEdge> outEdges = new HashSet<>();
    private final Set<Neo4JEdge> inEdges = new HashSet<>();
    private final SortedSet<String> labelsAdded = new TreeSet<>();
    private final SortedSet<String> labelsRemoved = new TreeSet<>();
    private final SortedSet<String> labels;
    private final String idFieldName;
    private final Object id;

    private boolean outEdgesLoaded = false;
    private boolean inEdgesLoaded = false;
    private boolean dirty = false;
    private SortedSet<String> matchLabels;

    Neo4JVertex(Graph graph, Neo4JSession session, Neo4JElementIdProvider propertyIdProvider, String idFieldName, Object id, Collection<String> labels) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(propertyIdProvider, "propertyIdProvider cannot be null");
        Objects.requireNonNull(idFieldName, "idFieldName cannot be null");
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(labels, "labels cannot be null");
        // store fields
        this.graph = graph;
        this.session = session;
        this.propertyIdProvider = propertyIdProvider;
        this.idFieldName = idFieldName;
        this.id = id;
        this.labels = new TreeSet<>(labels);
        // this is the original set of labels (used to match the vertex)
        this.matchLabels = new TreeSet<>(labels);
    }

    Neo4JVertex(Graph graph, Neo4JSession session, Neo4JElementIdProvider propertyIdProvider, String idFieldName, Node node) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(propertyIdProvider, "propertyIdProvider cannot be null");
        Objects.requireNonNull(idFieldName, "idFieldName cannot be null");
        Objects.requireNonNull(node, "node cannot be null");
        // store fields
        this.graph = graph;
        this.session = session;
        this.propertyIdProvider = propertyIdProvider;
        this.idFieldName = idFieldName;
        // from node
        this.id = node.get(idFieldName).asObject();
        this.labels = StreamSupport.stream(node.labels().spliterator(), false).collect(Collectors.toCollection(TreeSet::new));
        // this is the original set of labels (used to match the vertex)
        this.matchLabels = new TreeSet<>(this.labels);
        // copy properties from node, remove idFieldName from map
        StreamSupport.stream(node.keys().spliterator(), false).filter(key -> idFieldName.compareTo(key) != 0).forEach(key -> {
            // value
            Value value = node.get(key);
            // process value type
            switch (value.type().name()) {
                case "LIST":
                    // process values
                    properties.put(key, value.asList().stream().map(item -> new Neo4JVertexProperty<>(this, propertyIdProvider.generateId(), key, item)).collect(Collectors.toList()));
                    // cardinality
                    cardinalities.put(key, VertexProperty.Cardinality.list);
                    break;
                case "MAP":
                    throw new RuntimeException("TODO: implement maps");
                default:
                    // add property
                    properties.put(key, Collections.singletonList(new Neo4JVertexProperty<>(this, propertyIdProvider.generateId(), key, value.asObject())));
                    // cardinality
                    cardinalities.put(key, VertexProperty.Cardinality.single);
                    break;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object id() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String label() {
        // labels separated by "::"
        return labels.stream().collect(Collectors.joining(Neo4JSession.VertexLabelDelimiter));
    }

    public String[] labels() {
        return labels.toArray(new String[labels.size()]);
    }

    public void addLabel(String label) {
        Objects.requireNonNull(label, "label cannot be null");
        // add label to set
        if (labels.add(label)) {
            // notify session
            session.dirtyVertex(this);
            // we need to update labels
            labelsAdded.add(label);
        }
    }

    public void removeLabel(String label) {
        Objects.requireNonNull(label, "label cannot be null");
        // remove label from set
        if (labels.remove(label)) {
            // check this label was previously added in this session
            if (!labelsAdded.remove(label)) {
                // notify session
                session.dirtyVertex(this);
                // we need to update labels
                labelsRemoved.add(label);
            }
        }
    }

    /**
     * Generates a Cypher MATCH pattern for the vertex, example:
     * <p>
     * (alias:Label1:Label2{id: {idParameterName}})
     * </p>
     *
     * @param alias           The node alias, <code>null</code> if not required.
     * @param idParameterName The name of the parameter that contains the vertex id.
     * @return the Cypher MATCH clause.
     */
    public String matchPattern(String alias, String idParameterName) {
        Objects.requireNonNull(idParameterName, "idParameterName cannot be null");
        // generate match pattern
        return alias != null ? "(" + alias + ":" + processLabels(matchLabels) + "{" + idFieldName + ": {" + idParameterName + "}})" : "(:" + processLabels(matchLabels) + "{" + idFieldName + ": {" + idParameterName + "}})";
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge addEdge(String label, Vertex vertex, Object... keyValues) {
        ElementHelper.validateLabel(label);
        if (vertex == null)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        // add edge
        return session.addEdge(label, this, (Neo4JVertex)vertex, keyValues);
    }

    void removeEdge(Neo4JEdge edge) {
        // remove edge from internal references
        outEdges.remove(edge);
        inEdges.remove(edge);
    }

    private void processEdgesWhereClause(String[] labels, List<Object> identifiers, String alias, StringBuilder builder, Map<String, Object> parameters) {
        // check identifiers are empty
        if (!identifiers.isEmpty()) {
            // filter edges
            builder.append(" WHERE not ").append(alias).append(".").append(idFieldName).append(" IN {ids}");
            // ids parameters
            parameters.put("ids", identifiers);
            // append labels filter if needed
            if (labels.length > 1) {
                // append condition
                builder.append(" AND type(r) IN {labels}");
                // labels parameters
                parameters.put("labels", labels);
            }
        }
        else if (labels.length > 1) {
            // append condition
            builder.append(" WHERE type(r) IN {labels}");
            // labels parameters
            parameters.put("labels", labels);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Edge> edges(Direction direction, String... labels) {
        Objects.requireNonNull(direction, "direction cannot be null");
        Objects.requireNonNull(labels, "labels cannot be null");
        // load labels in hash set
        Set<String> set = new HashSet<>(Arrays.asList(labels));
        // parameters
        Map<String, Object> parameters = new HashMap<>();
        // vertex id
        parameters.put("id", id);
        // out edges
        if (direction == Direction.OUT) {
            // check we have all edges in memory
            if (!outEdgesLoaded) {
                // create string builder
                StringBuilder builder = new StringBuilder();
                // match clause
                builder.append("MATCH ").append(matchPattern("n", "id")).append("-[r").append(labels.length == 1 ? ":`" + labels[0] + "`" : "").append("]->(m)");
                // edge ids already in memory
                List<Object> identifiers = outEdges.stream().map(Neo4JEdge::id).collect(Collectors.toList());
                // process where clause
                processEdgesWhereClause(labels, identifiers, "r", builder, parameters);
                // return
                builder.append(" return n, r, m");
                // create statement
                Statement statement = new Statement(builder.toString(), parameters);
                // execute command
                Stream<Edge> query = session.edges(statement);
                // edges in memory plus the ones in database (return copy since edges can be deleted in the middle of the loop)
                Iterator<Edge> result = Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(edge -> (Edge)edge), query)
                    .collect(Collectors.toList())
                    .iterator();
                // after this line it is safe to update loaded flag
                outEdgesLoaded = labels.length == 0;
                // return iterator
                return result;
            }
            // edges in memory (return copy since edges can be deleted in the middle of the loop)
            return outEdges.stream().filter(edge -> labels.length == 0 || set.contains(edge.label()))
                .map(edge -> (Edge)edge)
                .collect(Collectors.toList())
                .iterator();
        }
        // in edges
        if (direction == Direction.IN) {
            // check we have all edges in memory
            if (!inEdgesLoaded) {
                // create string builder
                StringBuilder builder = new StringBuilder();
                // match clause
                builder.append("MATCH ").append(matchPattern("n", "id")).append("<-[r").append(labels.length == 1 ? ":`" + labels[0] + "`" : "").append("]-(m)");
                // edge ids already in memory
                List<Object> identifiers = inEdges.stream().map(Neo4JEdge::id).collect(Collectors.toList());
                // process where clause
                processEdgesWhereClause(labels, identifiers, "r", builder, parameters);
                // return
                builder.append(" return n, r, m");
                // create statement
                Statement statement = new Statement(builder.toString(), parameters);
                // execute command
                Stream<Edge> query = session.edges(statement);
                // edges in memory plus the ones in database (return copy since edges can be deleted in the middle of the loop)
                Iterator<Edge> result = Stream.concat((labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(edge -> (Edge)edge), query)
                    .collect(Collectors.toList())
                    .iterator();
                // after this line it is safe to update loaded flag
                inEdgesLoaded = labels.length == 0;
                // return iterator
                return result;
            }
            // edges in memory (return copy since edges can be deleted in the middle of the loop)
            return inEdges.stream().filter(edge -> labels.length == 0 || set.contains(edge.label()))
                .map(edge -> (Edge)edge)
                .collect(Collectors.toList())
                .iterator();
        }
        // check we have all edges in memory
        if (!outEdgesLoaded || !inEdgesLoaded) {
            // create string builder
            StringBuilder builder = new StringBuilder();
            // match clause
            builder.append("MATCH ").append(matchPattern("n", "id")).append("-[r").append(labels.length == 1 ? ":`" + labels[0] + "`" : "").append("]-(m)");
            // edge ids already in memory
            List<Object> identifiers = Stream.concat(outEdges.stream(), inEdges.stream()).map(Neo4JEdge::id).collect(Collectors.toList());
            // process where clause
            processEdgesWhereClause(labels, identifiers, "r", builder, parameters);
            // return
            builder.append(" return n, r, m");
            // create statement
            Statement statement = new Statement(builder.toString(), parameters);
            // execute command
            Stream<Edge> query = session.edges(statement);
            // edges in memory plus the ones in database (return copy since edges can be deleted in the middle of the loop)
            Iterator<Edge> result = Stream.concat(Stream.concat(labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream(), labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(edge -> (Edge)edge), query)
                .collect(Collectors.toList())
                .iterator();
            // after this line it is safe to update loaded flags
            outEdgesLoaded = outEdgesLoaded || labels.length == 0;
            inEdgesLoaded = inEdgesLoaded || labels.length == 0;
            // return iterator
            return result;
        }
        // edges in memory (return copy since edges can be deleted in the middle of the loop)
        return Stream.concat(labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream(), labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream())
            .map(edge -> (Edge)edge)
            .collect(Collectors.toList())
            .iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction, String... labels) {
        Objects.requireNonNull(direction, "direction cannot be null");
        Objects.requireNonNull(labels, "labels cannot be null");
        // load labels in hash set
        Set<String> set = new HashSet<>(Arrays.asList(labels));
        // parameters
        Map<String, Object> parameters = new HashMap<>();
        // vertex id
        parameters.put("id", id);
        // out edges
        if (direction == Direction.OUT) {
            // check we have all edges in memory
            if (!outEdgesLoaded) {
                // create string builder
                StringBuilder builder = new StringBuilder();
                // match clause
                builder.append("MATCH ").append(matchPattern(null, "id")).append("-[r").append(labels.length == 1 ? ":`" + labels[0] + "`" : "").append("]->(m)");
                // edge ids already in memory
                List<Object> identifiers = outEdges.stream().map(Neo4JEdge::id).collect(Collectors.toList());
                // process where clause
                processEdgesWhereClause(labels, identifiers, "r", builder, parameters);
                // return
                builder.append(" return m");
                // create statement
                Statement statement = new Statement(builder.toString(), parameters);
                // execute command
                Stream<Vertex> query = session.vertices(statement);
                // return copy since elements can be deleted in the middle of the loop
                return Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex), query)
                    .collect(Collectors.toList())
                    .iterator();
            }
            // edges in memory (return copy since elements can be deleted in the middle of the loop)
            return (labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex)
                .collect(Collectors.toList())
                .iterator();
        }
        // in edges
        if (direction == Direction.IN) {
            // check we have all edges in memory
            if (!inEdgesLoaded) {
                // create string builder
                StringBuilder builder = new StringBuilder();
                // match clause
                builder.append("MATCH ").append(matchPattern(null, "id")).append("<-[r").append(labels.length == 1 ? ":`" + labels[0] + "`" : "").append("]-(m)");
                // edge ids already in memory
                List<Object> identifiers = inEdges.stream().map(Neo4JEdge::id).collect(Collectors.toList());
                // process where clause
                processEdgesWhereClause(labels, identifiers, "r", builder, parameters);
                // return
                builder.append(" return m");
                // create statement
                Statement statement = new Statement(builder.toString(), parameters);
                // execute command
                Stream<Vertex> query = session.vertices(statement);
                // return copy since elements can be deleted in the middle of the loop
                return Stream.concat((labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex), query)
                    .collect(Collectors.toList())
                    .iterator();
            }
            // edges in memory (return copy since elements can be deleted in the middle of the loop
            return (labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex)
                .collect(Collectors.toList())
                .iterator();
        }
        // check we have all edges in memory
        if (!outEdgesLoaded || !inEdgesLoaded) {
            // create string builder
            StringBuilder builder = new StringBuilder();
            // match clause
            builder.append("MATCH ").append(matchPattern(null, "id")).append("-[r").append(labels.length == 1 ? ":`" + labels[0] + "`" : "").append("]-(m)");
            // edge ids already in memory
            List<Object> identifiers = Stream.concat(outEdges.stream(), inEdges.stream()).map(Neo4JEdge::id).collect(Collectors.toList());
            // process where clause
            processEdgesWhereClause(labels, identifiers, "r", builder, parameters);
            // return
            builder.append(" return m");
            // create statement
            Statement statement = new Statement(builder.toString(), parameters);
            // execute command
            Stream<Vertex> query = session.vertices(statement);
            // return copy since elements can be deleted in the middle of the loop
            return Stream.concat(Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex), (labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex)), query)
                .collect(Collectors.toList())
                .iterator();
        }
        // edges in memory (return copy since edges can be deleted in the middle of the loop)
        return Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex), (labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex))
            .collect(Collectors.toList())
            .iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String name, V value, Object... keyValues) {
        ElementHelper.validateProperty(name, value);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        // check key values
        if (keyValues.length != 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        // check cardinality
        VertexProperty.Cardinality existingCardinality = cardinalities.get(name);
        if (existingCardinality != null && existingCardinality != cardinality)
            throw new IllegalArgumentException(String.format(Locale.getDefault(), "Property %s has been defined with %s cardinality", name, existingCardinality));
        // vertex property
        Neo4JVertexProperty<V> property = new Neo4JVertexProperty<>(this, propertyIdProvider.generateId(), name, value);
        // check cardinality
        switch (cardinality) {
            case list:
                // get existing list for key
                Collection<VertexProperty> list = properties.get(name);
                if (list == null) {
                    // initialize list
                    list = new ArrayList<>();
                    // use list
                    properties.put(name, list);
                    // cardinality
                    cardinalities.put(name, VertexProperty.Cardinality.list);
                }
                // add value to list, this will always call dirty method in session
                if (list.add(property)) {
                    // notify session
                    session.dirtyVertex(this);
                    // update flag
                    dirty = true;
                }
                break;
            case set:
                // get existing set for key
                Collection<VertexProperty> set = properties.get(name);
                if (set == null) {
                    // initialize set
                    set = new HashSet<>();
                    // use set
                    properties.put(name, set);
                    // cardinality
                    cardinalities.put(name, VertexProperty.Cardinality.list);
                }
                // add value to set, this will call dirty method in session only if element did not exist
                if (set.add(property)) {
                    // notify session
                    session.dirtyVertex(this);
                    // update flag
                    dirty = true;
                }
                break;
            default:
                // use value (single)
                properties.put(name, Collections.singletonList(property));
                // cardinality
                cardinalities.put(name, VertexProperty.Cardinality.single);
                // notify session
                session.dirtyVertex(this);
                // update flag
                dirty = true;
                break;
        }
        // return property
        return property;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> VertexProperty<V> property(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        // check we have a property with the given key
        Collection<?> collection = properties.get(key);
        if (collection != null) {
            // check size
            if (collection.size() == 1) {
                // iterator
                Iterator<?> iterator = collection.iterator();
                // advance iterator to first element
                if (iterator.hasNext()) {
                    // first element
                    return (VertexProperty<V>)iterator.next();
                }
                return null;
            }
            // exception
            throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        }
        return VertexProperty.<V>empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Objects.requireNonNull(propertyKeys, "propertyKeys cannot be null");
        // check we have properties with key
        if (!properties.isEmpty()) {
            // no properties in filter
            if (propertyKeys.length == 0) {
                // all properties (return a copy since properties iterator can be modified by calling remove())
                return properties.entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream())
                    .map(item -> (VertexProperty<V>)item)
                    .collect(Collectors.toList())
                    .iterator();
            }
            // one property in filter
            if (propertyKeys.length == 1) {
                // get list for key
                Collection<?> list = properties.get(propertyKeys[0]);
                if (list != null) {
                    // all properties (return a copy since properties iterator can be modified by calling remove())
                    return list.stream()
                        .map(item -> (VertexProperty<V>)item)
                        .collect(Collectors.toList())
                        .iterator();
                }
                // nothing on key
                return Collections.emptyIterator();
            }
            // loop property keys (return a copy since properties iterator can be modified by calling remove())
            return Arrays.stream(propertyKeys)
                .flatMap(key -> ((Collection<?>)properties.getOrDefault(key, Collections.EMPTY_LIST)).stream())
                .map(item -> (VertexProperty<V>)item)
                .collect(Collectors.toList())
                .iterator();
        }
        // nothing
        return Collections.emptyIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Graph graph() {
        return graph;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
        // remove all edges
        outEdges.forEach(edge -> session.removeEdge(edge, false));
        // remove vertex on session
        session.removeVertex(this);
    }

    void addInEdge(Neo4JEdge edge) {
        Objects.requireNonNull(edge, "edge cannot be null");
        // add to set
        inEdges.add(edge);
    }

    void addOutEdge(Neo4JEdge edge) {
        Objects.requireNonNull(edge, "edge cannot be null");
        // add to set
        outEdges.add(edge);
    }

    private Map<String, Object> statementParameters() {
        // define collector
        Collector<Map.Entry<String, Collection<VertexProperty>>, Map<String, Object>, Map<String, Object>> collector = Collector.of(
            HashMap::new,
            (map, entry) -> {
                // key & value
                String key = entry.getKey();
                Collection<VertexProperty> list = entry.getValue();
                // check cardinality
                if (cardinalities.get(key) == VertexProperty.Cardinality.single) {
                    // iterator
                    Iterator<VertexProperty> iterator = list.iterator();
                    // add single value to map
                    if (iterator.hasNext())
                        map.put(key, iterator.next().value());
                }
                else {
                    // add list of values
                    map.put(key, list.stream().map(Property::value).collect(Collectors.toList()));
                }
            },
            (map1, map2) -> map1,
            (map) -> map
        );
        // process properties
        Map<String, Object> parameters = properties.entrySet().stream().collect(collector);
        // append id
        parameters.put(idFieldName, id);
        // return parameters
        return parameters;
    }

    @Override
    public Statement insertStatement() {
        try {
            // create statement
            String statement = String.format(Locale.US, "CREATE (:%s{vp})", processLabels(this.labels));
            // parameters
            Value parameters = Values.parameters("vp", statementParameters());
            // command statement
            return new Statement(statement, parameters);
        }
        finally {
            // reset dirty flags
            dirty = false;
            labelsAdded.clear();
            labelsRemoved.clear();
            matchLabels = new TreeSet<>(labels);
        }
    }

    @Override
    public Statement updateStatement() {
        // check we need to issue statement (adding a label and then removing it will set the vertex as dirty in session but nothing to do)
        if (dirty || !labelsAdded.isEmpty() || !labelsRemoved.isEmpty()) {
            try {
                // create builder
                StringBuilder builder = new StringBuilder();
                // parameters
                Map<String, Object> parameters = new HashMap<>();
                // merge statement
                builder.append("MERGE ").append(matchPattern("v", "id"));
                // id parameter
                parameters.put("id", id);
                // check vertex is dirty
                if (dirty) {
                    // set properties
                    builder.append(" ON MATCH SET v = {vp}");
                    // update parameters
                    parameters.put("vp", statementParameters());
                }
                // check labels were added
                if (!labelsAdded.isEmpty()) {
                    // add labels
                    builder.append(!dirty ? " ON MATCH SET v:" : ", v:").append(processLabels(labelsAdded));
                }
                // check labels were removed
                if (!labelsRemoved.isEmpty()) {
                    // remove labels
                    builder.append("REMOVE v:").append(processLabels(labelsRemoved));
                }
                // command statement
                return new Statement(builder.toString(), parameters);
            }
            finally {
                // reset dirty flags
                dirty = false;
                labelsAdded.clear();
                labelsRemoved.clear();
                matchLabels = new TreeSet<>(labels);
            }
        }
        return null;
    }

    @Override
    public Statement deleteStatement() {
        // create statement
        String statement = "MATCH " + matchPattern("v", "id") + " DETACH DELETE v";
        // parameters
        Value parameters = Values.parameters("id", id);
        // command statement
        return new Statement(statement, parameters);
    }

    private static String processLabels(Set<String> labels) {
        Objects.requireNonNull(labels, "labels cannot be null");
        // process labels
        return labels.stream().map(label -> "`" + label + "`").collect(Collectors.joining(":"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        return object instanceof Vertex && ElementHelper.areEqual(this, object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
