package org.gloryjie.scheduler.core;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.core.filter.FullLogFilter;
import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

public class DagEngineProvide {


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
        return Lists.newArrayList(new FullLogFilter());
    }

}
