package org.gloryjie.scheduler.resilience4j;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

public class EasySchedulerResilience4jRegistry {


    public static final CircuitBreakerRegistry CIRCUIT_BREAKER_REGISTRY = CircuitBreakerRegistry.ofDefaults();


}
