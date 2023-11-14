package org.gloryjie.scheduler.api;

import java.util.concurrent.TimeUnit;

public interface DagEngine {

    /**
     * Synchronously schedules the DAG node.
     *
     * @param dagGraph The DAG graph containing the node.
     * @param context  The execution context for the graph.
     * @return The result of scheduling the dag.
     */
    DagResult fire(DagGraph dagGraph, Object context);


    /**
     * Synchronously schedules the DAG node with a timeout.
     *
     * @param dagGraph the DAG graph to schedule
     * @param context  the context object
     * @param timeout  the timeout in milliseconds
     * @return the result of scheduling the DAG node
     */
    DagResult fire(DagGraph dagGraph, Object context, Long timeout);

}
