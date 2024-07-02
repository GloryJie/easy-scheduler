package org.gloryjie.scheduler.api;

public interface NodeResult {


    /**
     * Returns the name of the node.
     *
     * @return the name of the node
     */
    String getNodeName();

    /**
     * Returns the state of the node.
     *
     * @return the state of the node.
     */
    NodeState getState();


    /**
     * Returns the throwable encountered during node execution.
     *
     * @return the throwable encountered during node execution
     */
    Throwable getThrowable();

    /**
     * Returns the cost time of the node in milliseconds.
     *
     * @return the cost time in milliseconds
     */
    Long getCostTime();

}
