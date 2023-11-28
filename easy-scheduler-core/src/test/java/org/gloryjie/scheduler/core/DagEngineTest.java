package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class DagEngineTest {

    /**
     * test one node succeeded
     */
    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void fireSingleNodeGraphSucceededTest(DagEngine dagEngine) {

        AtomicInteger testExecute = new AtomicInteger(0);
        DagGraph dagGraph = buildOneNodeGraph((context -> {
            testExecute.set(1);
            return "nodeResult";
        }));

        DagResult fireResult = dagEngine.fire(dagGraph, "testContext");

        assertTrue(fireResult.isDone());
        assertSame(DagState.SUCCEED, fireResult.getState());
        assertNull(fireResult.getThrowable());
        assertSame(NodeState.SUCCEEDED, fireResult.getNodeStateMap().get("A"));
        assertSame(1, testExecute.get());
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void fireSingleNodeGraphFailedTest(DagEngine dagEngine) {
        DagGraph dagGraph = buildOneNodeGraph((context -> {
            throw new RuntimeException("node execute error");
        }));

        DagResult fireResult = dagEngine.fire(dagGraph, "testContext");

        assertTrue(fireResult.isDone());
        assertSame(DagState.FAILED, fireResult.getState());
        assertInstanceOf(RuntimeException.class, fireResult.getThrowable());
        assertSame(NodeState.FAILED, fireResult.getNodeStateMap().get("A"));
    }


    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void fireSingleNodeGraphTimeoutTest(DagEngine dagEngine) {
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


    static Stream<Arguments> dagEngineProvider() {
        return Stream.of(
                Arguments.of(new ConcurrentDagEngine()),
                Arguments.of(new SingleThreadDagEngine())
        );
    }
}