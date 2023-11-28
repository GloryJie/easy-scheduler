package org.gloryjie.scheduler.api;

import java.util.List;
import java.util.Map;


public interface DagGraph {

    String START_NODE_NAME = "#START_NODE";
    String END_NODE_NAME = "#END_NODE";


    /**
     * Gets the name of the graph.
     *
     * @return the name of the graph
     */
    String getGraphName();

    /**
     * Returns a list of all nodes in the graph.
     *
     * @return a list of all nodes in the graph
     */
    List<DagNode<?>> nodes();

    /**
     * Returns a node by its unique name.
     *
     * @param nodeName The unique name of the node.
     * @return The node corresponding to the given name, or null if no node is found.
     */
    DagNode<?> getNode(String nodeName);

    /**
     * Returns the end node of the directed acyclic graph (DAG).
     * only one end node in the dag.
     *
     * @return The end node of the DAG.
     */
    DagNode<?> getEndNode();

    /**
     * Returns the start node of the Directed Acyclic Graph (DAG).
     * only one start node in the dag.
     *
     * @return The start node of the DAG.
     */
    DagNode<?> getStartNode();

    /**
     * Retrieves the in-degree information for all nodes.
     *
     * @return a map where the keys are node names and the values are the in-degrees of the nodes
     */
    Map<String, Integer> getNodeInDegree();

    /**
     * Retrieves all successor nodes of the specified node.
     *
     * @param nodeName The name of the node.
     * @return A list of successor nodes.
     */
    List<DagNode<?>> getSuccessorNodes(String nodeName);

    DependencyType getNodeDepencencyType(String src, String end);


    default Long timeout() {
        return null;
    }


    /**
     * Retrieves the value of the attribute associated with the specified key.
     *
     * @param key the key of the attribute
     * @return the value of the attribute, or null if the attribute does not exist
     */
    Object getAttribute(String key);

    /**
     * Sets the attribute with the specified key to the given value.
     *
     * @param key   the key of the attribute
     * @param value the value of the attribute
     */
    void setAttribute(String key, Object value);

    /**
     * Removes the attribute with the given key.
     *
     * @param key the key of the attribute to remove
     */
    void removeAttribute(String key);

}
