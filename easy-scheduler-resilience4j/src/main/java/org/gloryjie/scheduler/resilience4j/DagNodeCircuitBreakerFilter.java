package org.gloryjie.scheduler.resilience4j;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.apache.commons.lang3.StringUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;

public class DagNodeCircuitBreakerFilter extends AbstractCircuitBreakerFilter {

    public DagNodeCircuitBreakerFilter(CircuitBreakerRegistry circuitBreakerRegistry) {
        super(circuitBreakerRegistry);
    }

    @Override
    CircuitBreaker findCiruitBreaker(DagNode node, DagContext dagContext) {
        String nodeName = node.getNodeName();
        if (StringUtils.isNotBlank(nodeName)) {
            return this.circuitBreakerRegistry.find(nodeName).orElse(null);
        }
        return null;
    }

    @Override
    public int getOrder() {
        return Resilience4jFilterOrder.NODE_CIRCUIT_BREAKER_ORDER;
    }

}
