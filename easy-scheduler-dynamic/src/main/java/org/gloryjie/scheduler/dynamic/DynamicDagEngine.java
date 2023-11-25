package org.gloryjie.scheduler.dynamic;

import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.reader.DagGraphConfigType;
import org.gloryjie.scheduler.reader.DagGraphFactory;
import org.gloryjie.scheduler.reader.HandlerRegistry;

public interface DynamicDagEngine extends DagEngine, HandlerRegistry, GraphRegistry {


    /**
     * Fires a DagGraph with the specified context.
     *
     * @param context the context used to create the DagGraph
     * @return the result of firing the DagGraph
     * @throws DagEngineException if the DagGraph cannot be created
     */
    DagResult fireContext(Object context);


    DagResult fireConfig(String configName, Object context);

    void registerMethodHandler(Object bean);

    DagGraphFactory getDagGraphFactory();


}
