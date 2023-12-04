package org.gloryjie.scheduler.core;

import lombok.ToString;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.DependencyType;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@ToString(exclude = {"handler"})
public class DefaultDagNode<T> implements DagNode<T> {

    private final String nodeName;
    private final NodeHandler<T> handler;

    private final Map<String, DependencyType> dependencyMap = new ConcurrentHashMap<>();

    private Long timeout;

    private final ConcurrentHashMap<String, Object> attributeMap = new ConcurrentHashMap<>();


    public DefaultDagNode(String nodeName, NodeHandler<T> handler, Map<String, DependencyType> dependencyMap, Long timeout) {
        Objects.requireNonNull(nodeName, "node name must not be null");
        this.nodeName = nodeName;
        this.handler = handler;
        this.timeout = timeout;
        if (dependencyMap != null) {
            this.dependencyMap.putAll(dependencyMap);
        }
    }


    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    @Override
    public NodeHandler<T> getHandler() {
        return this.handler;
    }

    @Override
    public void addDependency(DependencyType dependencyType, String nodeName) {
        Objects.requireNonNull(dependencyType, "depend type must not be null");
        Objects.requireNonNull(nodeName, "node name must not be null");
        this.dependencyMap.put(nodeName, dependencyType);
    }

    @Override
    public void removeDependency(String nodeName) {
        Objects.requireNonNull(nodeName, "node name must not be null");
        this.dependencyMap.remove(nodeName);
    }

    @Override
    public Set<String> dependNodeNames() {
        // copy a new set to avoid modification
        return new HashSet<>(dependencyMap.keySet());
    }

    @Override
    public Map<String, DependencyType> dependNodeTypeMap() {
        return new HashMap<>(dependencyMap);
    }

    @Override
    public Object getAttribute(String key) {
        Objects.requireNonNull(key, "dag node attribute key must not be null");
        return attributeMap.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        Objects.requireNonNull(key, "dag node attribute key must not be null");
        Objects.requireNonNull(value, "dag node attribute value must not be null");
        attributeMap.put(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        Objects.requireNonNull(key, "dag node attribute key must not be null");
        attributeMap.remove(key);
    }

    @Override
    public Long timeout() {
        return this.timeout != null && this.timeout > 0 ? this.timeout : handler.timeout();
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {

        private String nodeName;
        private NodeHandler<T> handler;
        private final Map<String, DependencyType> dependTypeMap = new HashMap<>();

        private final Map<String, Object> attributes = new HashMap<>();
        private Long timeout;

        Builder() {

        }

        public Builder<T> nodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public Builder<T> handler(NodeHandler<T> handler) {
            this.handler = handler;
            return this;
        }

        public Builder<T> dependOn(String... nodeNames) {
            for (String name : nodeNames) {
                this.dependTypeMap.put(name, DependencyType.STRONG);
            }
            return this;
        }

        public Builder<T> dependOn(DependencyType dependencyType, String... nodeNames) {
            for (String name : nodeNames) {
                this.dependTypeMap.put(name, dependencyType);
            }
            return this;
        }

        public Builder<T> timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder<T> attribute(String key, Object value) {
            this.attributes.put(key, value);
            return this;
        }

        public DagNode<T> build() {
            DefaultDagNode<T> dagNode = new DefaultDagNode<>(nodeName, handler, dependTypeMap, timeout);
            this.attributes.forEach(dagNode::setAttribute);
            return dagNode;
        }
    }

}
