package org.gloryjie.scheduler.core;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.api.DagResult;

@Slf4j
public class SingleThreadDagEngine implements DagEngine {

    ConcurrentDagEngine concurrentDagEngine = new ConcurrentDagEngine(null);


    @Override
    public DagResult fire(DagGraph dagGraph, Object context, Long timeout) {
        return concurrentDagEngine.fire(dagGraph, context, timeout);
    }

    @Override
    public void registerFilter(DagNodeFilter filter) {
        concurrentDagEngine.registerFilter(filter);
    }

}
