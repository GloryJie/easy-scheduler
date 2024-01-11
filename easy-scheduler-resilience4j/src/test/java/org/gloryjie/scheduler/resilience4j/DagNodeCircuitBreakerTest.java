package org.gloryjie.scheduler.resilience4j;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.api.DagState;
import org.gloryjie.scheduler.core.DagGraphBuilder;
import org.gloryjie.scheduler.core.DefaultDagNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class DagNodeCircuitBreakerTest extends DagEngineProvide {

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void fireSingleNodeGraphSucceededTest(DagEngine dagEngine) throws Exception {

        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        DagGraph dagGraph = new DagGraphBuilder().graphName("CircuitBreakerTest")
                .addNode(DefaultDagNode.builder()
                        .nodeName("A")
                        .handler((dagNode, context) -> {
                            if (atomicBoolean.get()) {
                                throw new IllegalArgumentException("node execute error");
                            }
                            return null;
                        }).build()
                ).build();

        // new CircuitBreader A
        int waitDurationInOpenState = 1;
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .waitDurationInOpenState(Duration.ofSeconds(waitDurationInOpenState)).build();
        Optional<CircuitBreaker> optional = circuitBreakerRegistry.find("A");
        if (optional.isPresent()) {
            circuitBreakerRegistry.remove("A");
        }
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("A", config);


        for (int i = 0; i < 130; i++) {
            if (i == 110) {
                // i == 110, sleep 6s, open -> half-open
                TimeUnit.SECONDS.sleep(waitDurationInOpenState + 1);
                atomicBoolean.set(false);
            }
            DagResult fireResult = dagEngine.fire(dagGraph, "testContext");
            assertTrue(fireResult.isDone());

            if (i < 100) {
                // minimumNumberOfCalls == 100
                // 0 ~ 99, node faile, closed -> open
                assertSame(DagState.FAILED, fireResult.getState());
                assertTrue(fireResult.getThrowable() instanceof IllegalArgumentException);
            } else if (i < 110) {
                // 100 ~ 109, open state, not permitted
                assertEquals(circuitBreaker.getState(), CircuitBreaker.State.OPEN);
                assertSame(DagState.FAILED, fireResult.getState());
                // open state
                assertTrue(fireResult.getThrowable() instanceof CallNotPermittedException);
            } else if (i < 119) {
                // 110 ~ 119, execute success, half-open -> closed
                // when i == 119, permittedNumberOfCalls == 10, so open -> half-open
                assertEquals(circuitBreaker.getState(), CircuitBreaker.State.HALF_OPEN);

                // half-open state, 10 execute
                assertSame(DagState.SUCCEED, fireResult.getState());
            } else {
                // 120 ~ 129, closed state
                assertEquals(circuitBreaker.getState(), CircuitBreaker.State.CLOSED);
                // clased state, 10 execute
                assertSame(DagState.SUCCEED, fireResult.getState());
            }


        }
    }


}
