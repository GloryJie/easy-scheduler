package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SoftDependencyTypeTest extends DagEngineProvide {


    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void dNodeSoftDependATest(DagEngine dagEngine) {

        // a execute failed
        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("A", (node, context) -> {
                throw new RuntimeException("A node execute error");
            });
        }};

        // D soft depend A
        Map<String, Map<String, DependencyType>> dependMap = new HashMap<String, Map<String, DependencyType>>() {
            {
                put("D", new HashMap<String, DependencyType>() {{
                    put("A", DependencyType.SOFT);
                }});
            }
        };

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, dependMap);
        DagResult dagResult = dagEngine.fire(dagGraph, null);

        // D soft depend on A, strong depend on B, could succeed
        assertEquals(DagState.SUCCEED, dagResult.getState());
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("A"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void dNodeSoftDependTest(DagEngine dagEngine) {

        // a execute failed
        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("A", (node, context) -> {
                throw new RuntimeException("A node execute error");
            });
            put("B", (node, context) -> {
                throw new RuntimeException("B node execute error");
            });
        }};

        // D soft depend A
        Map<String, Map<String, DependencyType>> dependMap = new HashMap<String, Map<String, DependencyType>>() {
            {
                put("D", new HashMap<String, DependencyType>() {{
                    put("A", DependencyType.SOFT);
                    put("B", DependencyType.SOFT);
                }});
                put("E", new HashMap<String, DependencyType>() {{
                    put("B", DependencyType.SOFT);
                }});
            }
        };

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, dependMap);
        DagResult dagResult = dagEngine.fire(dagGraph, null);

        // D soft depend on A and B
        // E soft depend on B
        // could succeed
        assertEquals(DagState.SUCCEED, dagResult.getState());
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("A"));
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("B"));
    }


    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void gNodeSoftDependDTest(DagEngine dagEngine) {

        // a execute failed
        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("A", (node, context) -> {
                throw new RuntimeException("A node execute error");
            });
            put("B", (node, context) -> {
                throw new RuntimeException("B node execute error");
            });
            put("D", (node, context) -> {
                throw new RuntimeException("D node execute error");
            });
            put("E", (node, context) -> {
                throw new RuntimeException("E node execute error");
            });
        }};

        // D soft depend A
        Map<String, Map<String, DependencyType>> dependMap = new HashMap<String, Map<String, DependencyType>>() {
            {
                put("D", new HashMap<String, DependencyType>() {{
                    put("A", DependencyType.SOFT);
                    put("B", DependencyType.SOFT);
                }});
                put("E", new HashMap<String, DependencyType>() {{
                    put("B", DependencyType.SOFT);
                }});
                put("G", new HashMap<String, DependencyType>() {{
                    put("E", DependencyType.SOFT);
                }});
            }
        };

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, dependMap);
        DagResult dagResult = dagEngine.fire(dagGraph, null);

        // G soft depend on D, but strong depend on E，failed
        assertEquals(DagState.FAILED, dagResult.getState());
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("D"));
    }

    @ParameterizedTest
    @MethodSource("dagEngineProvider")
    public void gNodeSoftDependTest(DagEngine dagEngine) {

        // a execute failed
        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>() {{
            put("A", (node, context) -> {
                throw new RuntimeException("A node execute error");
            });
            put("B", (node, context) -> {
                throw new RuntimeException("B node execute error");
            });
            put("D", (node, context) -> {
                throw new RuntimeException("D node execute error");
            });
            put("E", (node, context) -> {
                throw new RuntimeException("E node execute error");
            });
        }};

        // D soft depend A
        Map<String, Map<String, DependencyType>> dependMap = new HashMap<String, Map<String, DependencyType>>() {
            {
                put("D", new HashMap<String, DependencyType>() {{
                    put("A", DependencyType.SOFT);
                    put("B", DependencyType.SOFT);
                }});
                put("E", new HashMap<String, DependencyType>() {{
                    put("B", DependencyType.SOFT);
                }});
                put("G", new HashMap<String, DependencyType>() {{
                    put("E", DependencyType.SOFT);
                    put("D", DependencyType.SOFT);
                }});
                put("F", new HashMap<String, DependencyType>() {{
                    put("D", DependencyType.SOFT);
                }});
            }
        };

        DagGraph dagGraph = BaseGraph.buidGraph(actionMap, dependMap);
        DagResult dagResult = dagEngine.fire(dagGraph, null);

        // G soft depend on D，E
        // F soft depend on D
        // could succeed
        assertEquals(DagState.SUCCEED, dagResult.getState());
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("D"));
        assertEquals(NodeState.FAILED, dagResult.getNodeStateMap().get("E"));
        assertEquals(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("G"));
    }

}
