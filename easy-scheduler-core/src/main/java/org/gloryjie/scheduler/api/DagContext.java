package org.gloryjie.scheduler.api;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public interface DagContext {

    String NODE_RESULT_PREFIX = "#NODE_RESULT#";

    String USER_CONTEXT = "#USER_CONTEXT#";

    /**
     * Retrieves the result of a node from the context.
     *
     * @param nodeName The name of the node. Must not be null.
     * @return The result of the node.
     */
    default NodeResult<?> getNodeResult(String nodeName) {
        Object value = this.get(NODE_RESULT_PREFIX + nodeName);
        return value == null ? null : (NodeResult<?>) value;
    }

    /**
     * Puts the node result into the context.
     *
     * @param nodeName The name of the node. Must not be null.
     * @param result   The result of the node. Must not be null.
     */
    default void putNodeResult(String nodeName, NodeResult<?> result) {
        Objects.requireNonNull(nodeName, "nodeName must not null");
        Objects.requireNonNull(result, "nodeResult must not null");
        this.put(NODE_RESULT_PREFIX + nodeName, result);
    }

    /**
     * Retrieves the user context object.
     *
     * @return The user context object.
     */
    @Nullable
    default Object getContext() {
        return this.get(USER_CONTEXT);
    }

    /**
     * Puts a key-value pair into the object.
     *
     * @param key   the key for the value
     * @param value the value to be stored
     * @return the previous value associated with the key, or null if there was no mapping for the key
     */
    @CanIgnoreReturnValue
    Object put(String key, Object value);

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key The key to retrieve the value for.
     * @return The value associated with the key, or null if the key is not found.
     */
    @Nullable
    Object get(String key);

    /**
     * Removes a key-value from the contxt based on the specified key.
     *
     * @param key the key of the object to remove
     * @return the removed object, or null if the key was not found
     */
    @CanIgnoreReturnValue
    Object remove(String key);


    Map<String,Object> asMap();

}
