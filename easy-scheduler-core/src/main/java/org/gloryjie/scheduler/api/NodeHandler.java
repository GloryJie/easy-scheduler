package org.gloryjie.scheduler.api;


public interface NodeHandler<R> {

    /**
     * Returns the name of the handler.
     *
     * @return the name of the handler
     */
    default String handlerName() {
        return "";
    }


    /**
     * Evaluates the DAG node with the given context.
     *
     * @param dagNode    The DAG node to evaluate.
     * @param dagContext The context for evaluation.
     * @return True if the evaluation is successful, false otherwise.
     */
    default boolean evaluate(DagNode<Object> dagNode, DagContext dagContext) {
        return true;
    }


    /**
     * Executes the given dagNode with the provided dagContext.
     *
     * @param dagNode    The dagNode to be executed.
     * @param dagContext The dagContext to be used during execution.
     * @return The result of the execution.
     */
    R execute(DagNode<Object> dagNode, DagContext dagContext);


    /**
     * Returns the timeout value for executing the node.
     *
     * @return The timeout value in milliseconds. Returns null if there is no timeout control.
     */
    default Long timeout() {
        return null;
    }


}
