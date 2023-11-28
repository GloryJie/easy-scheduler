package org.gloryjie.scheduler.api;

import java.util.Map;
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
    default void addDependency(String nodeName) {
        addDependency(DependencyType.STRONG, nodeName);
    }

    void addDependency(DependencyType dependencyType, String nodeName);

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

    Map<String, DependencyType> dependNodeTypeMap();

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
