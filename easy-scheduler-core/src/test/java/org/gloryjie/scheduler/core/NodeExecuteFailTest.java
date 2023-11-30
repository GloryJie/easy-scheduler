package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NodeExecuteFailTest {


    /**
     * A node execute fail
     *
     * @param dagEngine
     */
    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void aNodeExecuteFailTest(DagEngine dagEngine) {
        Map<String, NodeHandler> actionMap = new HashMap<>();

        actionMap.put("A", (node, context) -> {
            throw new RuntimeException("A node execute error");
        });

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);

        DagResult dagResult = dagEngine.fire(dagGraph, null);
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("A"));
        assertEquals("A node execute error", dagResult.getThrowable().getMessage());
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void dNodeExecuteFailTest(DagEngine dagEngine) {
        Map<String, NodeHandler> actionMap = new HashMap<>();

        actionMap.put("D", (node, context) -> {
            throw new RuntimeException("D node execute error");
        });

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);

        DagResult dagResult = dagEngine.fire(dagGraph, null);
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals("D node execute error", dagResult.getThrowable().getMessage());

        // A & B must succeed
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("A"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("B"));

        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("D"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void eNodeExecuteFailTest(DagEngine dagEngine) {
        Map<String, NodeHandler> actionMap = new HashMap<>();

        actionMap.put("E", (node, context) -> {
            throw new RuntimeException("E node execute error");
        });

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);

        DagResult dagResult = dagEngine.fire(dagGraph, null);
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals("E node execute error", dagResult.getThrowable().getMessage());

        // C & B must succeed
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("B"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("C"));

        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("E"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void fNodeExecuteFailTest(DagEngine dagEngine) {
        Map<String, NodeHandler> actionMap = new HashMap<>();

        actionMap.put("F", (node, context) -> {
            throw new RuntimeException("F node execute error");
        });

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);

        DagResult dagResult = dagEngine.fire(dagGraph, null);
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals("F node execute error", dagResult.getThrowable().getMessage());

        // a, b, d, e must succeed
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("A"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("B"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("D"));

        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("F"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void gNodeExecuteFailTest(DagEngine dagEngine) {
        Map<String, NodeHandler> actionMap = new HashMap<>();

        actionMap.put("G", (node, context) -> {
            throw new RuntimeException("G node execute error");
        });

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);

        DagResult dagResult = dagEngine.fire(dagGraph, null);
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals("G node execute error", dagResult.getThrowable().getMessage());

        // a, b, c, d, e must succeed
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("A"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("B"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("C"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("D"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("E"));

        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("G"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void hNodeExecuteFailTest(DagEngine dagEngine) {
        Map<String, NodeHandler> actionMap = new HashMap<>();

        actionMap.put("H", (node, context) -> {
            throw new RuntimeException("H node execute error");
        });

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);

        DagResult dagResult = dagEngine.fire(dagGraph, null);
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals("H node execute error", dagResult.getThrowable().getMessage());

        // a, b, c, d, e must succeed
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("C"));

        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("H"));
    }


    static Stream<Arguments> dagEngineProvider() {
        return Stream.of(
                Arguments.of(new ConcurrentDagEngine()),
                Arguments.of(new SingleThreadDagEngine())
        );
    }

}
