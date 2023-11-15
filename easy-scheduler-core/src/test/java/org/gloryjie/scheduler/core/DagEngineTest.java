package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class DagEngineTest {

    private DagEngine dagEngine = new ConcurrentDagEngine();




    /**
     * test one node succeeded
     */
    @Test
    public void fireSingleNodeGraphSucceededTest() {

        AtomicInteger testExecute = new AtomicInteger(0);
        DagGraph dagGraph = buildOneNodeGraph((context -> {
            testExecute.set(1);
            return "nodeResult";
        }));

        DagResult fireResult = dagEngine.fire(dagGraph, "testContext");

        assertTrue(fireResult.isDone());
        assertSame(DagState.SUCCESS, fireResult.getState());
        assertNull(fireResult.getThrowable());
        assertSame(NodeState.SUCCEEDED, fireResult.getNodeStateMap().get("A"));
        assertSame(1, testExecute.get());
    }

    @Test
    public void fireSingleNodeGraphFailedTest() {
        DagGraph dagGraph = buildOneNodeGraph((context -> {
            throw new RuntimeException("node execute error");
        }));

        DagResult fireResult = dagEngine.fire(dagGraph, "testContext");

        assertTrue(fireResult.isDone());
        assertSame(DagState.FAILED, fireResult.getState());
        assertInstanceOf(RuntimeException.class, fireResult.getThrowable());
        assertSame(NodeState.FAILED, fireResult.getNodeStateMap().get("A"));
    }


    @Test
    public void fireSingleNodeGraphTimeoutTest() {
        DagGraph dagGraph = buildOneNodeGraph((context -> {
            try {
                TimeUnit.SECONDS.sleep(1);
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        DagResult fireResult = dagEngine.fire(dagGraph, "testContext", 800L);

        assertTrue(fireResult.isDone());
        assertEquals(DagState.TIMEOUT, fireResult.getState());
        assertInstanceOf(TimeoutException.class, fireResult.getThrowable());

        //not check node state, could be timeout or running state
    }

    @SuppressWarnings({"all"})
    private DagGraph buildOneNodeGraph(Function<DagContext, Object> action) {
        NodeHandler printHandler = DefaultNodeHandler.builder()
                .handlerName("A")
                .when(context -> context.getContext() != null)
                .action(action)
                .build();

        DagNode dagNodeA = DefaultDagNode.builder().nodeName("A").handler(printHandler).build();

        DagGraph dagGraph = new DagGraphBuilder()
                .graphName("test")
                .addNodes(dagNodeA)
                .build();

        return dagGraph;
    }
}