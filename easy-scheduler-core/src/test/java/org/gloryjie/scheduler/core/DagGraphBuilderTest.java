package org.gloryjie.scheduler.core;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DagGraphBuilderTest {


    /**
     * test build a graph with only one node
     * startNode -> A -> endNode
     */
    @Test
    public void oneNodeGraphTest() {
        NodeHandler<String> printHandler = DefaultNodeHandler.<String>builder()
                .handlerName("A")
                .when(pairs -> pairs.getContext() != null)
                .action(pairs -> "hello world")
                .build();

        DagNode<String> dagNodeA = DefaultDagNode.<String>builder().nodeName("A").handler(printHandler).build();

        DagGraph dagGraph = new DagGraphBuilder()
                .graphName("test")
                .addNodes(dagNodeA)
                .build();

        assertNotNull(dagGraph.getStartNode());
        assertNotNull(dagGraph.getEndNode());
        assertNotNull(dagGraph.getNode("A"));
        assertNotNull(dagGraph.getNodeInDegree());

        Map<String, Integer> nodeInDegreeInfo = dagGraph.getNodeInDegree();
        assertEquals(0, nodeInDegreeInfo.get(DagGraph.START_NODE_NAME));
        assertEquals(1, nodeInDegreeInfo.get("A"));
        assertEquals(1, nodeInDegreeInfo.get(DagGraph.END_NODE_NAME));

        assertNotNull(dagGraph.getSuccesorNodes(DagGraph.START_NODE_NAME));
        assertSame(dagGraph.getSuccesorNodes(DagGraph.START_NODE_NAME).get(0), dagGraph.getNode("A"));
        assertNotNull(dagGraph.getSuccesorNodes("A"));
        assertSame(dagGraph.getEndNode(), dagGraph.getSuccesorNodes("A").get(0));
        assertTrue(dagGraph.getSuccesorNodes(DagGraph.END_NODE_NAME).isEmpty());
    }


    /**
     * test build a graph with only two nodes
     * startNode -> A  -> endNode
     * -> B  -> endNode
     */
    @Test
    public void twoNodeGraphTest() {
        NodeHandler<String> printHandler = DefaultNodeHandler.<String>builder()
                .handlerName("A")
                .when(pairs -> pairs.getContext() != null)
                .action(pairs -> "hello world")
                .build();

        DagNode<String> dagNodeA = DefaultDagNode.<String>builder().nodeName("A").handler(printHandler).build();
        DagNode<String> dagNodeB = DefaultDagNode.<String>builder().nodeName("B").handler(printHandler).build();

        DagGraph dagGraph = new DagGraphBuilder().graphName("test").addNodes(dagNodeA, dagNodeB).build();

        assertNotNull(dagGraph.getStartNode());
        assertNotNull(dagGraph.getEndNode());
        assertNotNull(dagGraph.getNode("A"));
        assertNotNull(dagGraph.getNode("B"));
        assertNotNull(dagGraph.getNodeInDegree());

        Map<String, Integer> nodeInDegreeInfo = dagGraph.getNodeInDegree();
        assertEquals(0, nodeInDegreeInfo.get(DagGraph.START_NODE_NAME));
        assertEquals(1, nodeInDegreeInfo.get("A"));
        assertEquals(1, nodeInDegreeInfo.get("B"));
        assertEquals(2, nodeInDegreeInfo.get(DagGraph.END_NODE_NAME));

        assertNotNull(dagGraph.getSuccesorNodes(DagGraph.START_NODE_NAME));

        List<String> startSuccessorNodes = dagGraph.getSuccesorNodes(DagGraph.START_NODE_NAME)
                .stream().map(DagNode::getNodeName).collect(Collectors.toList());
        assertTrue(CollectionUtils.containsAll(startSuccessorNodes, Lists.newArrayList("A", "B")));

        assertNotNull(dagGraph.getSuccesorNodes("A"));
        assertNotNull(dagGraph.getSuccesorNodes("B"));
        assertSame(dagGraph.getEndNode(), dagGraph.getSuccesorNodes("A").get(0));
        assertSame(dagGraph.getEndNode(), dagGraph.getSuccesorNodes("B").get(0));
        assertTrue(dagGraph.getSuccesorNodes(DagGraph.END_NODE_NAME).isEmpty());
    }


    /**
     * test build a graph with only two nodes
     * startNode -> A  -> B -> endNode
     */
    @Test
    public void twoNodeGraphBTest() {
        NodeHandler<String> printHandler = DefaultNodeHandler.<String>builder()
                .handlerName("A")
                .when(pairs -> pairs.getContext() != null)
                .action(pairs -> "hello world")
                .build();

        DagNode<String> dagNodeA = DefaultDagNode.<String>builder().nodeName("A").handler(printHandler).build();
        DagNode<String> dagNodeB = DefaultDagNode.<String>builder().nodeName("B")
                .dependOn("A").handler(printHandler).build();

        DagGraph dagGraph = new DagGraphBuilder()
                .graphName("test")
                .addNodes(dagNodeA, dagNodeB)
                .build();
        System.out.println(dagGraph.toString());
        assertNotNull(dagGraph.getStartNode());
        assertNotNull(dagGraph.getEndNode());
        assertNotNull(dagGraph.getNode("A"));
        assertNotNull(dagGraph.getNode("B"));
        assertNotNull(dagGraph.getNodeInDegree());

        Map<String, Integer> nodeInDegreeInfo = dagGraph.getNodeInDegree();
        assertEquals(0, nodeInDegreeInfo.get(DagGraph.START_NODE_NAME));
        assertEquals(1, nodeInDegreeInfo.get("A"));
        assertEquals(1, nodeInDegreeInfo.get("B"));
        assertEquals(1, nodeInDegreeInfo.get(DagGraph.END_NODE_NAME));

        assertNotNull(dagGraph.getSuccesorNodes(DagGraph.START_NODE_NAME));

        List<String> startSuccessorNodes = dagGraph.getSuccesorNodes(DagGraph.START_NODE_NAME)
                .stream().map(DagNode::getNodeName).collect(Collectors.toList());
        assertTrue(CollectionUtils.containsAll(startSuccessorNodes, Lists.newArrayList("A")));

        assertNotNull(dagGraph.getSuccesorNodes("A"));
        assertNotNull(dagGraph.getSuccesorNodes("B"));
        assertSame(dagGraph.getNode("B"), dagGraph.getSuccesorNodes("A").get(0));
        assertSame(dagGraph.getEndNode(), dagGraph.getSuccesorNodes("B").get(0));
        assertTrue(dagGraph.getSuccesorNodes(DagGraph.END_NODE_NAME).isEmpty());
    }


    /**
     * test build a graph with cycle dependency
     */
    @Test
    public void buildGraphWithCycleTest() {
        NodeHandler<String> printHandler = DefaultNodeHandler.<String>builder()
                .handlerName("printHandler")
                .when(pairs -> pairs.getContext() != null)
                .action(pairs -> "hello world")
                .build();

        // A -> B
        // B -> A
        DagNode<String> dagNodeA = DefaultDagNode.<String>builder()
                .nodeName("A").handler(printHandler).dependOn("B").build();
        DagNode<String> dagNodeB = DefaultDagNode.<String>builder()
                .nodeName("B").dependOn("A").handler(printHandler).build();

        assertThrows(IllegalArgumentException.class,
                () -> new DagGraphBuilder()
                        .graphName("test")
                        .addNodes(dagNodeA, dagNodeB)
                        .build());

        // A->B
        dagNodeA.removeDependency("B");
        assertDoesNotThrow(() -> new DagGraphBuilder()
                .graphName("test")
                .addNodes(dagNodeA, dagNodeB)
                .build());

    }


}