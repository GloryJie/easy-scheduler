package org.gloryjie.scheduler.api;


public interface NodeHandler<R> {

    String handlerName();


    /**
     * evaluate handler could execute
     */
    default boolean evaluate(DagContext dagContext) {
        return true;
    }

    /**
     * node execute method
     */
    R execute(DagContext dagContext) throws Exception;


    /**
     * node execute timeout config
     * @return no timeout control return null
     */
    default Long timeout(){
      return null;
    }


}
