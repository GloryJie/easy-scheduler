package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// @SuppressWarnings("all")
public class DefaultNodeHandlerTest {


    @Test
    public void defaultNodeHandlerEvaluateTest() {
        NodeHandler<Object> handler = DefaultNodeHandler.builder()
                .handlerName("test")
                .when(dagContext -> true)
                .action(dagContext -> {
                    System.out.println("hello world: " + dagContext);
                    return "hello";
                }).build();

        DagContext dagContext = new MapDagContext(null);

        handler.evaluate(null, dagContext);

        try {
            handler.execute(null, dagContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    public void defaultNodeHandlerEvaluateExceptionTest() {
        NodeHandler<Object> handler = DefaultNodeHandler.builder()
                .handlerName("test")
                .when(dagContext -> true)
                .action(dagContext -> {
                    System.out.println("hello world: " + dagContext);
                    throw new RuntimeException("test exception");
                }).build();

        DagContext dagContext = new MapDagContext(null);

        handler.evaluate(null, dagContext);

        Exception exception = null;
        try {
            handler.execute(null, dagContext);
        } catch (Exception e) {
            exception = e;

        }

        Assertions.assertNotNull(exception);
        Assertions.assertInstanceOf(RuntimeException.class, exception);
    }

}