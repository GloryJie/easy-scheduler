package org.gloryjie.scheduler.api;

/**
 * node execute
 */
public enum DagState {

    WAITING,
    RUNNING,
    /**
     * All node execute success
     */
    SUCCESS,
    /**
     * at least one node execute failed
     */
    FAILED,
    /**
     * execute timeout
     */
    TIMEOUT,
    /**
     * interrupted
     */
    INTERRUPTED;

    /**
     * Checks if the given DagState is a done state.
     *
     * @param dagState The DagState to check
     * @return true if the DagState is a done state, false otherwise
     */
    public static boolean isDoneState(DagState dagState) {
        return dagState == DagState.SUCCESS
                || dagState == DagState.FAILED
                || dagState == DagState.TIMEOUT
                || dagState == DagState.INTERRUPTED;

    }


}
