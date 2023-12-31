package org.gloryjie.scheduler.core;

import lombok.ToString;
import org.gloryjie.scheduler.api.DagContext;

import java.util.HashMap;
import java.util.Map;

@ToString
public class MapDagContext implements DagContext {

    private final HashMap<String, Object> map = new HashMap<>();

    public MapDagContext(Object userContext) {
        this.put(DagContext.USER_CONTEXT, userContext);
    }

    @Override
    public Object put(String key, Object value) {
        return map.put(key, value);
    }

    @Override
    public Object get(String key) {
        return map.get(key);
    }

    @Override
    public Object remove(String key) {
        return map.remove(key);
    }

    @Override
    public Map<String, Object> asMap() {
        return new HashMap<>(map);
    }


}
