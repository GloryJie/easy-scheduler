package org.gloryjie.scheduler.core;

import com.google.common.graph.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.gloryjie.scheduler.api.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings("all")
public class DagGraphBuilder {

    public static final int DEFAULT_WEIGHT = 1;

    private String graphName;

    /**
     * guava graph
     * string is node type, use string to avoid dagNode which need to imple equals and hashcode
     * integer is edge weight
     */
    private MutableValueGraph<String, Integer> mutableValueGraph;

    private final Map<String, DagNode<?>> nodeMap;

    private DagNode<?> startNode;

    private DagNode<?> endNode;

    private Long timeout;

    private Consumer<DagContext> initMethod;

    private Consumer<DagContext> endMethod;

    private Map<String, Object> attributes;

    public DagGraphBuilder() {
        mutableValueGraph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        nodeMap = new HashMap<>();
        attributes = new HashMap<>();
    }

    public DagGraphBuilder graphName(String graphName) {
        this.graphName = graphName;
        return this;
    }

    public DagGraphBuilder timeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public DagGraphBuilder addNodes(DagNode<?>... dagNodes) {
        Objects.requireNonNull(dagNodes, "dagNodes must not be null");
        Arrays.stream(dagNodes).forEach(this::addNode);
        return this;
    }


    public <T> DagGraphBuilder addNode(DagNode<T> dagNode) {
        Objects.requireNonNull(dagNode, "dagNode must not be null");
        Objects.requireNonNull(dagNode.getNodeName(), "dagNode name must not be null");
        if (nodeMap.containsKey(dagNode.getNodeName())) {
            throw new IllegalArgumentException("dagNode name must be unique");
        }
        nodeMap.put(dagNode.getNodeName(), dagNode);
        return this;
    }

    public DagGraphBuilder init(Consumer<DagContext> consumer) {
        this.initMethod = consumer;
        return this;
    }

    public DagGraphBuilder end(Consumer<DagContext> consumer) {
        this.endMethod = consumer;
        return this;
    }

    public DagGraphBuilder attribute(String key, Object value) {
        this.attributes.put(key, value);
        return this;
    }

    public DagGraph build() {
        if (MapUtils.isEmpty(nodeMap)) {
            throw new IllegalArgumentException("dag graph must not be empty");
        }

        buildMutableValueGraph();

        checkGraphHasCycle();

        // build dag graph
        ImmutableValueGraph immutableValueGraph = ImmutableValueGraph.copyOf(mutableValueGraph);
        DefaultDagGraph dagGraph = new DefaultDagGraph(graphName, immutableValueGraph, nodeMap, timeout);
        this.attributes.forEach(dagGraph::setAttribute);
        return dagGraph;
    }


    private void buildMutableValueGraph() {
        for (Map.Entry<String, DagNode<?>> entry : nodeMap.entrySet()) {
            String nodeName = entry.getKey();
            DagNode<?> dagNode = entry.getValue();
            Map<String, DependencyType> dependNodeTypeMap = dagNode.dependNodeTypeMap();
            Set<String> dependencyDagNodeNames = dependNodeTypeMap.keySet();

            // add node
            mutableValueGraph.addNode(dagNode.getNodeName());

            // add edge
            for (String dependencyDagNodeName : dependencyDagNodeNames) {
                DagNode<?> dependNode = nodeMap.get(dependencyDagNodeName);
                if (dependNode == null) {
                    String errMsg = String.format("dependency node must not be null, node=%s, dependency=%s",
                            nodeName, dependencyDagNodeName);
                    throw new IllegalArgumentException(errMsg);
                }
                int weight = dependNodeTypeMap.getOrDefault(dependencyDagNodeName, DependencyType.STRONG).getCode();
                mutableValueGraph.putEdgeValue(dependNode.getNodeName(), dagNode.getNodeName(), weight);
            }
        }

        checkGraphHasCycle();

        // The start node and the end node converge into one node
        // connect to start node and end node
        connectToStartNode();
        connectToEndNode();

    }

    private void connectToStartNode() {
        Set<String> startNodes = mutableValueGraph.nodes()
                .stream()
                .filter(node -> mutableValueGraph.inDegree(node) == 0)
                .collect(Collectors.toSet());

        if (CollectionUtils.isEmpty(startNodes)) {
            throw new IllegalArgumentException("start nodes must not be empty");
        }


        NodeHandler<Object> startHandler = null;
        if (initMethod != null) {
            startHandler = DefaultNodeHandler.builder()
                    .handlerName(DagGraph.START_NODE_NAME)
                    .action((dagNode, dagContext) -> {
                        initMethod.accept(dagContext);
                        return null;
                    }).build();
        }

        startNode = DefaultDagNode.builder()
                .nodeName(DagGraph.START_NODE_NAME)
                .handler(startHandler)
                .build();

        nodeMap.put(DagGraph.START_NODE_NAME, startNode);
        mutableValueGraph.addNode(startNode.getNodeName());

        startNodes.forEach(node -> mutableValueGraph.putEdgeValue(startNode.getNodeName(), node, DEFAULT_WEIGHT));
    }

    private void connectToEndNode() {
        Set<String> leafNodes = new HashSet<>();
        findLeafNodes(startNode.getNodeName(), new HashSet<>(), leafNodes);

        if (CollectionUtils.isEmpty(leafNodes)) {
            throw new IllegalArgumentException("end nodes must not be empty");
        }

        NodeHandler<Object> endHandler = null;
        if (endMethod != null) {
            endHandler = DefaultNodeHandler.builder()
                    .handlerName(DagGraph.END_NODE_NAME)
                    .action(dagContext -> {
                        endMethod.accept(dagContext);
                        return null;
                    }).build();
        }
        endNode = DefaultDagNode.builder()
                .nodeName(DagGraph.END_NODE_NAME)
                .handler(endHandler)
                .build();
        nodeMap.put(DagGraph.END_NODE_NAME, endNode);
        mutableValueGraph.addNode(endNode.getNodeName());

        for (String leafNode : leafNodes) {
            mutableValueGraph.putEdgeValue(leafNode, endNode.getNodeName(), DEFAULT_WEIGHT);
        }
    }


    /**
     * dfs find leaf nodes
     */
    public void findLeafNodes(String node, Set<String> visited, Set<String> result) {
        visited.add(node);
        if (!mutableValueGraph.successors(node).isEmpty()) {
            for (String successor : mutableValueGraph.successors(node)) {
                if (!visited.contains(successor)) {
                    findLeafNodes(successor, visited, result);
                }
            }
        } else {
            result.add(node);
        }
    }

    private void checkGraphHasCycle() {
        Graph<String> graph = mutableValueGraph.asGraph();
        if (Graphs.hasCycle(graph)) {
            throw new IllegalArgumentException("graph has cycle");
        }
    }


}
