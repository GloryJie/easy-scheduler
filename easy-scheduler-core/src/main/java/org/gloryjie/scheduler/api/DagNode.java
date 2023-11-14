package org.gloryjie.scheduler.api;

import java.util.Set;

/**
 * dagNode describe
 *
 * @param <R>
 */
public interface DagNode<R> {

    /**
     * Returns the name of the node.
     *
     * @return the name of the node
     */
    String getNodeName();

    /**
     * Retrieves the handler for the node.
     *
     * @return the handler for the node
     */
    NodeHandler<R> getHandler();

    /**
     * Adds a dependency to the specified node.
     *
     * @param nodeName the name of the node to add the dependency to
     */
    void addDependency(String nodeName);

    /**
     * Removes a dependency for the given nodeName.
     *
     * @param nodeName the name of the node for which the dependency should be removed
     */
    void removeDependency(String nodeName);

    /**
     * Retrieves a set of node names that this dagNode depends on.
     *
     * @return A set of dependent node names.
     */
    Set<String> dependNodeNames();

    /**
     * Returns the timeout value of the function.
     * If the value is null, no timeout control is applied.
     *
     * @return the timeout value
     */
    default Long timeout() {
        //  Get the timeout value from the handler
        return getHandler().timeout();
    }

}
