package org.gloryjie.scheduler.resilience4j;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.apache.commons.lang3.StringUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.api.DagNodeInvoker;
import org.gloryjie.scheduler.core.DagEngineException;

import java.util.Optional;

public class CircuitBreakerFilter implements DagNodeFilter {

    public static final int ORDER = 100;

    @Override
    public Object invoke(DagNodeInvoker dagNodeInvoker, DagNode node, DagContext dagContext) {
        if (StringUtils.isEmpty(node.getNodeName())) {
            return dagNodeInvoker.invoke(node, dagContext);
        }

        Optional<CircuitBreaker> optional = EasySchedulerResilience4jRegistry
                .CIRCUIT_BREAKER_REGISTRY.find(node.getNodeName());
        if (optional.isPresent()) {
            try {
                return optional.get().executeCallable(() -> dagNodeInvoker.invoke(node, dagContext));
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new DagEngineException("[CircuitBreakerFilter] execute failed", e);
            }
        }

        return dagNodeInvoker.invoke(node, dagContext);
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

}
