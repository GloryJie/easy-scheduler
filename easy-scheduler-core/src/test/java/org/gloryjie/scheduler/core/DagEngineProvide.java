package org.gloryjie.scheduler.core;

import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

public class DagEngineProvide {


    static Stream<Arguments> dagEngineProvider() {
        return Stream.of(
                Arguments.of(new ConcurrentDagEngine()),
                Arguments.of(new SingleThreadDagEngine())
        );
    }

}
