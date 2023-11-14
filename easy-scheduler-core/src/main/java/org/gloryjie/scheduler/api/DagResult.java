package org.gloryjie.scheduler.api;

import java.util.Map;


public interface DagResult {

    DagState getState();

    /**
     * Returns a map of node states.
     *
     * @return The map containing node states.
     */
    Map<String, NodeState> getNodeStateMap();

    /**
     * Returns the throwable object if the DAG execution failed.
     *
     * @return the throwable object representing the failure
     */
    Throwable getThrowable();


    /**
     * Retrieves the cost time.
     *
     * @return the cost time in milliseconds
     */
    Long getCostTime();


    /**
     * Checks if the DAG execution is complete.
     *
     * @return true if the execution is done, false otherwise.
     */
    default boolean isDone() {
        return DagState.isDoneState(getState());
    }


}
