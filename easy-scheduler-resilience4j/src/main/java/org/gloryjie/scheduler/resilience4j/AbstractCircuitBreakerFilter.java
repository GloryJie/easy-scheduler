package org.gloryjie.scheduler.resilience4j;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.api.DagNodeInvoker;
import org.gloryjie.scheduler.core.DagEngineException;

public abstract class AbstractCircuitBreakerFilter implements DagNodeFilter {

    protected CircuitBreakerRegistry circuitBreakerRegistry;

    public AbstractCircuitBreakerFilter(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    @Override
    public Object invoke(DagNodeInvoker dagNodeInvoker, DagNode node, DagContext dagContext) {
        if (circuitBreakerRegistry == null) {
            return dagNodeInvoker.invoke(node, dagContext);
        }
        CircuitBreaker ciruitBreaker = this.findCiruitBreaker(node, dagContext);
        if (ciruitBreaker == null) {
            return dagNodeInvoker.invoke(node, dagContext);
        }


        try {
            return ciruitBreaker.executeCallable(() -> dagNodeInvoker.invoke(node, dagContext));
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new DagEngineException("[CircuitBreakerFilter] execute failed", e);
        }
    }


    abstract CircuitBreaker findCiruitBreaker(DagNode node, DagContext dagContext);
}
