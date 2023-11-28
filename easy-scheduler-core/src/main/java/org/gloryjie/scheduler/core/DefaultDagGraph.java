package org.gloryjie.scheduler.core;

import com.google.common.graph.ImmutableValueGraph;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Default implementation of DagGraph
 *
 */
@SuppressWarnings("all")
public class DefaultDagGraph implements DagGraph {

    private final String graphName;
    private final Map<String, DagNode<?>> dagNodeMap;
    private ImmutableValueGraph<String, Integer> graph;
    private final Map<String, Integer> nodeInDegreeMap;

    private Long timeout;

    private final ConcurrentHashMap<String, Object> attributeMap = new ConcurrentHashMap<>();

    public DefaultDagGraph(String graphName,
                           ImmutableValueGraph<String, Integer> graph,
                           Map<String, DagNode<?>> dagNodeMap,
                           Long timeout) {
        Objects.requireNonNull(graphName, "graph name must not be null");
        Objects.requireNonNull(graph, "graph must not be null");
        Objects.requireNonNull(dagNodeMap.get(DagGraph.START_NODE_NAME), "start node must not be null");
        Objects.requireNonNull(dagNodeMap.get(DagGraph.END_NODE_NAME), "end node must not be null");
        this.graphName = graphName;
        this.dagNodeMap = Collections.unmodifiableMap(new HashMap<>(dagNodeMap));
        this.graph = graph;
        nodeInDegreeMap = Collections.unmodifiableMap(graph.nodes().stream()
                .collect(Collectors.toMap(Function.identity(), graph::inDegree)));
        this.timeout = timeout;
    }


    @Override
    public String getGraphName() {
        return this.graphName;
    }

    @Override
    public List<DagNode<?>> nodes() {
        return new ArrayList<>(dagNodeMap.values());
    }

    @Override
    public DagNode<?> getNode(String nodeName) {
        return dagNodeMap.get(nodeName);
    }

    @Override
    public DagNode<?> getEndNode() {
        return dagNodeMap.get(DagGraph.END_NODE_NAME);
    }

    @Override
    public DagNode<?> getStartNode() {
        return dagNodeMap.get(DagGraph.START_NODE_NAME);
    }

    @Override
    public Map<String, Integer> getNodeInDegree() {
        return new HashMap<>(nodeInDegreeMap);
    }

    @Override
    public List<DagNode<?>> getSuccessorNodes(String nodeName) {
        // The node's successor reads from the Graph instead of the dagNode
        // because the dagNode dependency could be modified by the outside
        return graph.successors(nodeName).stream()
                .map(dagNodeMap::get)
                .collect(Collectors.toList());
    }

    @Override
    public Object getAttribute(String key) {
        Objects.requireNonNull(key, "dag graph attribute key must not be null");
        return attributeMap.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        Objects.requireNonNull(key, "dag graph attribute key must not be null");
        Objects.requireNonNull(value, "dag graph attribute value must not be null");
        attributeMap.put(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        Objects.requireNonNull(key, "dag graph attribute key must not be null");
        attributeMap.remove(key);
    }

    @Override
    public String toString() {
        return "DefaultDagGraph{" +
                "graphName='" + graphName + '\'' +
                ", dagNodeMap=" + dagNodeMap +
                ", graph=" + graph +
                ", nodeInDegreeMap=" + nodeInDegreeMap +
                ", timeout=" + timeout +
                ", attributeMap=" + attributeMap +
                '}';
    }
}
