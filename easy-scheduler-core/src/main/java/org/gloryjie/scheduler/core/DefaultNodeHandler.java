package org.gloryjie.scheduler.core;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

@EqualsAndHashCode(of = "name")
@ToString(of = "name")
public class DefaultNodeHandler<T> implements NodeHandler<T> {

    private final String name;

    private final Predicate<DagContext> when;

    private final Function<DagContext, T> action;

    private final Long timeout;

    public DefaultNodeHandler(String name, Function<DagContext, T> action) {
        this(name, null, action, null);
    }

    public DefaultNodeHandler(String name, Predicate<DagContext> when, Function<DagContext, T> action) {
        this(name, when, action, null);
    }

    public DefaultNodeHandler(String name, Predicate<DagContext> when, Function<DagContext, T> action, Long timeout) {
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
    public boolean evaluate(DagContext dagContext) {
        return when == null || when.test(dagContext);
    }


    @Override
    public T execute(DagContext dagContext) {
        return action == null ? null : action.apply(dagContext);
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
        private Predicate<DagContext> when;
        private Function<DagContext, T> action;
        private Long timeout;

        Builder() {
        }

        public Builder<T> handlerName(String name) {
            this.handlerName = name;
            return this;
        }

        public Builder<T> when(Predicate<DagContext> when) {
            this.when = when;
            return this;
        }

        public Builder<T> action(Function<DagContext, T> action) {
            this.action = action;
            return this;
        }

        public Builder<T> timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public NodeHandler<T> build() {
            return new DefaultNodeHandler<>(this.handlerName, this.when, this.action, this.timeout);
        }

        public String toString() {
            return "DefaultNodeHandler.Builder(handlerName=" + this.handlerName
                    + ", when=" + this.when + ", action=" + this.action + ", timeout=" + this.timeout + ")";
        }
    }

}
