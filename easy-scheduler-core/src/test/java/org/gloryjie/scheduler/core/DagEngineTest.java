package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class DagEngineTest extends DagEngineProvide {

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
        System.out.println(fireResult);
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
                .action((action))
                .build();

        DagNode dagNodeA = DefaultDagNode.builder().nodeName("A").handler(printHandler).build();

        DagGraph dagGraph = new DagGraphBuilder()
                .graphName("test")
                .addNodes(dagNodeA)
                .build();

        return dagGraph;
    }


    @Test
    public void helloWorldTest() {
        NodeHandler printHandler = DefaultNodeHandler.builder()
                .handlerName("printHandler")
                .when(context -> context.getContext() != null)
                .action((dagNode, dagContext) -> {
                    System.out.println("Hello DagNode: " + dagNode.getNodeName());
                    return null;
                }).build();

        DagNode dagNodeA = DefaultDagNode.builder().nodeName("A").handler(printHandler).build();

        DagNode dagNodeB = DefaultDagNode.builder().nodeName("B")
                .handler(printHandler).dependOn("A").build();

        DagGraph dagGraph = new DagGraphBuilder()
                .graphName("helloGraph")
                .addNodes(dagNodeA, dagNodeB)
                .build();

        DagEngine dagEngine = new ConcurrentDagEngine();

        DagResult dagResult = dagEngine.fire(dagGraph, "your context");
        // check result state
        if (dagResult.getState() == DagState.SUCCEED) {
            // do something
        } else {
            Throwable err = dagResult.getThrowable();
            // do something
        }


    }

    // @ParameterizedTest
    // @MethodSource("dagEngineProvider")
    @Test
    public void baseGraphTest() {

        List<String> nodeNameList = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H");


        Map<String, NodeHandler> actionMap = new HashMap<String, NodeHandler>();

        for (String nodeName : nodeNameList) {
            actionMap.put(nodeName, (node, context) -> {
                try {
                    if ("C".equals(nodeName) || "B".equals(nodeName)) {
                        return null;
                    }
                    int i = ThreadLocalRandom.current().nextInt(10);
                    TimeUnit.MILLISECONDS.sleep(i);
                } catch (InterruptedException e) {
                    //
                }
                return null;
            });
        }


        DagNode aNode = DefaultDagNode.builder().nodeName("A").handler(actionMap.get("A")).build();
        DagNode bNode = DefaultDagNode.builder().nodeName("B").handler(actionMap.get("B"))
                .dependOn("A").build();
        DagNode cNode = DefaultDagNode.builder().nodeName("C").handler(actionMap.get("C"))
                .dependOn("A").build();
        DagNode dNode = DefaultDagNode.builder().nodeName("D").handler(actionMap.get("D"))
                .dependOn("A", "B", "C").build();

        DagGraph dagGraph = new DagGraphBuilder().graphName("test")
                .timeout(80000000L)
                .end(dagContext -> {
                    try {
                        int i = ThreadLocalRandom.current().nextInt(10);
                        TimeUnit.MILLISECONDS.sleep(i);
                    } catch (InterruptedException e) {
                        //
                    }
                })
                .addNodes(aNode, bNode, cNode, dNode).build();

        DagEngine concurrentDagEngine = new ConcurrentDagEngine(new SingleExcutorSelector(300));
        int taskNum = 1;
        CountDownLatch countDownLatch = new CountDownLatch(taskNum);

        int threadNum = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            executorService.execute(() -> {
            });
        }

        for (int i = 0; i < taskNum; i++) {
            CompletableFuture.runAsync(() -> {
                try {
                    DagResult dagResult = concurrentDagEngine.fire(dagGraph, null);
                    System.out.println(dagResult);
                    if ((dagResult.getState() == DagState.FAILED)) {
                        System.out.println("err：" + dagResult.getThrowable());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    countDownLatch.countDown();
                }
            }, executorService);
        }

        try {
            countDownLatch.await();
            System.out.println("done");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}