package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.core.DagEngineException;

public interface DynamicFireDagEngine extends DagEngine {


    /**
     * Fires a DagGraph with the specified context.
     *
     * @param context the context used to create the DagGraph
     * @return the result of firing the DagGraph
     * @throws DagEngineException if the DagGraph cannot be created
     */
    DagResult fireContext(Object context);

}
