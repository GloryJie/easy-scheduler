package org.gloryjie.scheduler.api;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nullable;
import java.util.Map;

public interface DagContext {

    String USER_CONTEXT = "#USER_CONTEXT#";


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


    /**
     * Converts the context to a map.
     *
     * @return the context as a map
     */
    Map<String, Object> asMap();

}
