package org.gloryjie.scheduler.resilience4j;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.apache.commons.lang3.StringUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.Optional;

public class HandlerCircuitBreakerFilter extends AbstractCircuitBreakerFilter {

    public HandlerCircuitBreakerFilter(CircuitBreakerRegistry circuitBreakerRegistry) {
        super(circuitBreakerRegistry);
    }

    @Override
    CircuitBreaker findCiruitBreaker(DagNode node, DagContext dagContext) {
        String handlerName = Optional.ofNullable(node.getHandler()).map(NodeHandler::handlerName).orElse(null);
        if (StringUtils.isNotBlank(handlerName)) {
            return this.circuitBreakerRegistry.find(handlerName).orElse(null);
        }
        return null;
    }

    @Override
    public int getOrder() {
        return Resilience4jFilterOrder.HANDLER_CIRCUIT_BREAKER_ORDER;
    }

}
