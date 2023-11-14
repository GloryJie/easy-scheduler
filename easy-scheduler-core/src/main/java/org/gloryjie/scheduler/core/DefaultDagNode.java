package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import lombok.Builder;
import lombok.ToString;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@ToString(exclude = {"handler"})
public class DefaultDagNode<T> implements DagNode<T> {

    private final String nodeName;
    private final NodeHandler<T> handler;

    private final Set<String> dependencies;

    private Long timeout;

    public DefaultDagNode(String nodeName) {
        this(nodeName, null);
    }

    public DefaultDagNode(NodeHandler<T> handler) {
        this(Objects.requireNonNull(handler, "NodeHandler must not be null").handlerName(), handler);
    }

    public DefaultDagNode(String nodeName, NodeHandler<T> handler) {
        this(nodeName, handler, null);
    }

    public DefaultDagNode(String nodeName, NodeHandler<T> handler, Set<String> dependencies, Long timeout) {
        this(nodeName, handler, dependencies);
        this.timeout = timeout;
    }

    public DefaultDagNode(String nodeName, NodeHandler<T> handler, Set<String> dependencies) {
        Objects.requireNonNull(nodeName, "node name must not be null");
        this.nodeName = nodeName;
        this.handler = handler;
        if (dependencies != null) {
            this.dependencies = new HashSet<>(dependencies);
        } else {
            this.dependencies = new HashSet<>();
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
    public void addDependency(String nodeName) {
        Objects.requireNonNull(nodeName, "node name must not be null");
        this.dependencies.add(nodeName);
    }

    @Override
    public void removeDependency(String nodeName) {
        Objects.requireNonNull(nodeName, "node name must not be null");
        this.dependencies.remove(nodeName);
    }

    @Override
    public Set<String> dependNodeNames() {
        // copy a new set to avoid modification
        return new HashSet<>(dependencies);
    }


    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {

        private String nodeName;
        private NodeHandler<T> handler;
        private final Set<String> denpencies = new HashSet<>();

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
            this.denpencies.addAll(Arrays.asList(nodeNames));
            return this;
        }

        public Builder<T> timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public DagNode<T> build() {
            return new DefaultDagNode<>(nodeName, handler, denpencies, timeout);
        }
    }

}
