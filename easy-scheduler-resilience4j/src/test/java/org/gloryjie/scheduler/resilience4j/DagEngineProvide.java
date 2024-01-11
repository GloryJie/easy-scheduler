package org.gloryjie.scheduler.resilience4j;

import com.google.common.collect.Lists;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.SingleThreadDagEngine;
import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

public class DagEngineProvide {

    public static final CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();

    static Stream<Arguments> dagEngineProvider() {
        ConcurrentDagEngine concurrentDagEngine = new ConcurrentDagEngine();
        SingleThreadDagEngine singleThreadDagEngine = new SingleThreadDagEngine();

        for (DagNodeFilter dagNodeFilter : filterProvider()) {
            concurrentDagEngine.registerFilter(dagNodeFilter);
            singleThreadDagEngine.registerFilter(dagNodeFilter);
        }

        return Stream.of(
                Arguments.of(concurrentDagEngine),
                Arguments.of(singleThreadDagEngine)
        );
    }


    static List<DagNodeFilter> filterProvider() {
        return Lists.newArrayList(new DagNodeCircuitBreakerFilter(circuitBreakerRegistry));
    }

}
