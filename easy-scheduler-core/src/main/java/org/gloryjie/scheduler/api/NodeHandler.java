package org.gloryjie.scheduler.api;


public interface NodeHandler<R> {

    String handlerName();


    /**
     * evaluate handler could execute
     */
    default boolean evaluate(DagContext dagContext) {
        return true;
    }


    R execute(DagNode<Object> dagNode, DagContext dagContext);


    /**
     * node execute timeout config
     *
     * @return no timeout control return null
     */
    default Long timeout() {
        return null;
    }


}
