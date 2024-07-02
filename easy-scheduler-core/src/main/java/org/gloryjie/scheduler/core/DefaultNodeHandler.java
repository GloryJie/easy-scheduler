package org.gloryjie.scheduler.core;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

@EqualsAndHashCode(of = "name")
@ToString(of = "name")
public class DefaultNodeHandler implements NodeHandler {

    private final String name;

    private final BiPredicate<DagNode, DagContext> when;

    private BiConsumer<DagNode, DagContext> action;

    private final Long timeout;


    public DefaultNodeHandler(String name,
                              BiPredicate<DagNode, DagContext> when,
                              BiConsumer<DagNode, DagContext> action,
                              Long timeout) {
        Objects.requireNonNull(name, "NodeHandler name can not be null");
        this.name = name;
        this.when = when;
        this.action = action;
        this.timeout = timeout;
    }

    @Override
    public String handlerName() {
        return name;
    }

    @Override
    public boolean evaluate(DagNode dagNode, DagContext dagContext) {
        return when == null || when.test(dagNode, dagContext);
    }

    public void execute(DagNode dagNode, DagContext dagContext) {
        action.accept(dagNode, dagContext);
    }

    @Override
    public Long timeout() {
        return this.timeout;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }


    public static class Builder<T> {

        private String handlerName;
        private BiPredicate<DagNode, DagContext> when;
        private BiConsumer<DagNode, DagContext> action;

        private Long timeout;

        Builder() {
        }

        public Builder<T> handlerName(String name) {
            this.handlerName = name;
            return this;
        }

        public Builder<T> when(Predicate<DagContext> when) {
            this.when((dagNode, dagContext) -> when.test(dagContext));
            return this;
        }

        public Builder<T> when(BiPredicate<DagNode, DagContext> when) {
            if (this.when == null) {
                this.when = when;
            } else {
                this.when = this.when.and(when);
            }
            return this;
        }

        public Builder<T> action(Consumer<DagContext> action) {
            this.action((dagNode, dagContext) -> action.accept(dagContext));
            return this;
        }

        public Builder<T> action(BiConsumer<DagNode, DagContext> action) {
            this.action = action;
            return this;
        }

        public Builder<T> timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public NodeHandler build() {
            return new DefaultNodeHandler(this.handlerName, this.when, this.action, this.timeout);
        }

        public String toString() {
            return "DefaultNodeHandler.Builder(handlerName=" + this.handlerName
                    + ", when=" + this.when + ", action=" + this.action + ", timeout=" + this.timeout + ")";
        }
    }

}
