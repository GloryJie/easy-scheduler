package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NodeExecuteTimeoutTest extends DagEngineProvide {


    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void bNodeTimeoutLessThanGraphTest(DagEngine dagEngine) {

        // b execute timeout, sleep millseconds not greater than graph
        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("B", (node, context) -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(120);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }};

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);
        DagResult dagResult = dagEngine.fire(dagGraph, null);

        if (dagEngine instanceof SingleThreadDagEngine) {
            assertEquals(DagState.SUCCEED, dagResult.getState());
            return;
        }

        // concurrent execute
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals(NodeState.TIMEOUT, dagResult.getNodeStateMap().get("B"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void dNodeTimeoutLessThanGraphTest(DagEngine dagEngine) {

        // b execute timeout, sleep millseconds not greater than graph
        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("D", (node, context) -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(120);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }};

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);
        DagResult dagResult = dagEngine.fire(dagGraph, null);

        if (dagEngine instanceof SingleThreadDagEngine) {
            assertEquals(DagState.SUCCEED, dagResult.getState());
            return;
        }

        // concurrent execute
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("A"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("B"));
        assertEquals(NodeState.TIMEOUT, dagResult.getNodeStateMap().get("D"));

    }


    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void bNodeTimeoutGreatThanGraphTest(DagEngine dagEngine) {

        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("B", (node, context) -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(80);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            put("D", (node, context) -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(80);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            put("G", (node, context) -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(80);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }};

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, null);
        DagResult dagResult = dagEngine.fire(dagGraph, null);


        if (dagEngine instanceof SingleThreadDagEngine) {
            assertEquals(DagState.TIMEOUT, dagResult.getState());
            return;
        }

        // concurrent engine
        assertEquals(DagState.TIMEOUT, dagResult.getState());
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("B"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("D"));
        assertEquals(NodeState.RUNNING, dagResult.getNodeStateMap().get("G"));
    }


}
