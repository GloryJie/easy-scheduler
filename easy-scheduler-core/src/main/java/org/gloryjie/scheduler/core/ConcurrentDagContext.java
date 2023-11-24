package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.DagContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentDagContext implements DagContext {

    private final ConcurrentHashMap<String, Object> concurrentHashMap = new ConcurrentHashMap<>();

    public ConcurrentDagContext(Object userContext) {
        if (userContext != null){
            this.put(DagContext.USER_CONTEXT, userContext);
        }
    }

    @Override
    public Object put(String key, Object value) {
        Objects.requireNonNull(key, "key must not null");
        Objects.requireNonNull(value, "value must not null");
        return concurrentHashMap.put(key, value);
    }

    @Override
    public Object get(String key) {
        return concurrentHashMap.get(key);
    }

    @Override
    public Object remove(String key) {
        return concurrentHashMap.remove(key);
    }

    @Override
    public Map<String, Object> asMap() {
        return new HashMap<>(concurrentHashMap);
    }
}
