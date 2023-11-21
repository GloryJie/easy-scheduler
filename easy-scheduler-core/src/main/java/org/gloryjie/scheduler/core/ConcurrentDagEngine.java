package org.gloryjie.scheduler.core;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.gloryjie.scheduler.api.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@Slf4j
public class ConcurrentDagEngine implements DagEngine {

    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("ConcurrentDagEngineScheduled");
                thread.setDaemon(true);
                return thread;
            });
    private final ExecutorSelector executorSelector;


    public ConcurrentDagEngine() {
        this(new ExecutorSelector() {
            private final ExecutorService executorService =
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            @Override
            public ExecutorService select(String graphName) {
                return executorService;
            }
        });
    }

    public ConcurrentDagEngine(ExecutorSelector executorSelector) {
        this.executorSelector = executorSelector;
    }

    @Override
    public DagResult fire(DagGraph dagGraph, Object context) {
        return this.fire(dagGraph, context, null);
    }

    @Override
    public DagResult fire(DagGraph dagGraph, Object context, Long timeout) {
        ExecutorService executorService = executorSelector.select(dagGraph.getGraphName());
        ConcurrentDagContext dagContext = new ConcurrentDagContext(context);
        DagExecutor dagExecutor = new DagExecutor(executorService, dagGraph, dagContext, timeout);
        dagExecutor.start();
        return dagExecutor;
    }


    protected CompletableFuture<NodeResultImpl<Object>> wrapNodeFutureWithTimeout(
            DagNode<?> dagNode,
            NodeResultImpl<Object> nodeResult,
            CompletableFuture<NodeResultImpl<Object>> future
    ) {
        if (dagNode.timeout() == null || dagNode.timeout() <= 0) {
            return future;
        }

        CompletableFuture<NodeResultImpl<Object>> timeoutFuture = new CompletableFuture<>();

        scheduledExecutorService.schedule(() -> {
            if (!future.isDone()
                    && (nodeResult.getState() == NodeState.WAITING || nodeResult.getState() == NodeState.RUNNING)) {
                nodeResult.setState(NodeState.TIMEOUT);
                String msg = "DagNode[" + dagNode.getNodeName() + "] not completed in " + dagNode.timeout() + "ms";
                nodeResult.setThrowable(new TimeoutException(msg));
                future.complete(nodeResult);
            }
        }, dagNode.timeout(), TimeUnit.MILLISECONDS);

        return future.applyToEither(timeoutFuture, Function.identity());
    }


    @Getter
    @Setter
    private static class NodeResultImpl<T> implements NodeResult<T> {

        private final String nodeName;
        private T result;
        private volatile NodeState state;
        private Throwable throwable;
        private long startTime;
        private long endTime;
        private long submitTime;

        public NodeResultImpl(String nodeName) {
            this.nodeName = nodeName;
        }


        @Override
        public String getNodeName() {
            return nodeName;
        }

        @Override
        public T getResult() {
            return result;
        }

        @Override
        public NodeState getState() {
            return state;
        }

        @Override
        public Throwable getThrowable() {
            return throwable;
        }

        @Override
        public Long getCostTime() {
            return endTime - startTime;
        }
    }


    private class DagExecutor implements DagResult {

        private final ExecutorService executorService;
        private final DagGraph dagGraph;
        private final DagContext dagContext;
        private final Map<String, AtomicInteger> nodeInDegreeInfo;
        private final Long timeout;


        /**
         * Fields that change at run time
         */
        private final CountDownLatch countDownLatch;
        private final AtomicReference<DagState> dagStateRef = new AtomicReference<>(DagState.WAITING);
        private final ConcurrentHashMap<String, NodeState> nodeStateMap;
        private volatile Throwable throwable;
        private volatile long startTime;
        private volatile long endTime;

        public DagExecutor(ExecutorService executorService, DagGraph dagGraph, DagContext dagContext, Long timeout) {
            this.executorService = executorService;
            this.dagGraph = dagGraph;
            this.dagContext = dagContext;
            this.timeout = timeout;

            // init from dagGraph
            nodeInDegreeInfo = new ConcurrentHashMap<>();
            dagGraph.getNodeInDegree().forEach((nodeName, inDegree) ->
                    nodeInDegreeInfo.put(nodeName, new AtomicInteger(inDegree)));
            nodeStateMap = new ConcurrentHashMap<>();
            dagGraph.nodes().forEach(node -> nodeStateMap.put(node.getNodeName(), NodeState.WAITING));

            countDownLatch = new CountDownLatch(1);
        }

        public void start() {
            startTime = System.currentTimeMillis();
            dagStateRef.set(DagState.RUNNING);
            log.debug("Graph[{}] start", dagGraph.getGraphName());

            fireNode(dagGraph.getStartNode());

            // wait
            try {
                if (timeout == null) {
                    countDownLatch.await();
                } else {
                    boolean awaitResult = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                    if (!awaitResult) {
                        log.warn("Graph[{}] timeout", dagGraph.getGraphName());
                        String msg = String.format("Graph[%s] not completed in %s ms",
                                dagGraph.getGraphName(), timeout);
                        dagDone(DagState.TIMEOUT, new TimeoutException(msg));
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Graph[{}] interrupted", dagGraph.getGraphName());
                dagDone(DagState.INTERRUPTED, e);
            }
        }

        private void fireNextNode(DagNode<?> curNode) {
            List<DagNode<?>> successorNodes = dagGraph.getSuccessorNodes(curNode.getNodeName());
            if (curNode == dagGraph.getEndNode() || CollectionUtils.isEmpty(successorNodes)) {
                dagDone(DagState.SUCCESS, null);
            } else {
                decrementSuccessorInDegree(successorNodes);
                checkAndFireSuccessorNodes(successorNodes);
            }
        }

        private void decrementSuccessorInDegree(List<DagNode<?>> successorNodes) {
            for (DagNode<?> successorNode : successorNodes) {
                nodeInDegreeInfo.get(successorNode.getNodeName()).decrementAndGet();
            }
        }

        private void checkAndFireSuccessorNodes(List<DagNode<?>> successorNodes) {
            if (dagStateRef.get() != DagState.RUNNING) {
                return;
            }
            for (DagNode<?> successorNode : successorNodes) {
                String nodeName = successorNode.getNodeName();
                NodeState nodeState = nodeStateMap.get(nodeName);
                if (nodeState == NodeState.WAITING) {
                    int inDegree = nodeInDegreeInfo.get(nodeName).get();
                    if (inDegree == 0) {
                        fireNode(successorNode);
                    } else if (inDegree < 0) {
                        // safe check
                        dagDone(DagState.FAILED, new DagEngineException("inDegree could not be less than 0"));
                    }
                }
            }
        }

        private void dagDone(DagState state, Throwable throwable) {
            this.throwable = throwable;
            this.endTime = System.currentTimeMillis();
            this.dagStateRef.set(state);
            this.countDownLatch.countDown();
        }

        private void fireNode(DagNode<?> node) {
            NodeResultImpl<Object> nodeResult = new NodeResultImpl<>(node.getNodeName());
            nodeResult.setSubmitTime(System.currentTimeMillis());

            // Asynchronously execute the node
            CompletableFuture<NodeResultImpl<Object>> nodeFuture =
                    CompletableFuture.supplyAsync(() -> {
                        // check state
                        if (dagStateRef.get() != DagState.RUNNING
                                || nodeStateMap.get(node.getNodeName()) != NodeState.WAITING) {
                            return null;
                        }
                        nodeStateMap.put(node.getNodeName(), NodeState.RUNNING);

                        executeNode(node, nodeResult);

                        return nodeResult;
                    }, executorService);

            // Submit the node timeout future
            CompletableFuture<NodeResultImpl<Object>> timeoutFuture =
                    ConcurrentDagEngine.this.wrapNodeFutureWithTimeout(node, nodeResult, nodeFuture);

            timeoutFuture.thenAccept(curResult -> {
                        // if curResult is null, just return
                        // if dag state is not running, just return
                        // if node state is not running (timeout or other situation), just return
                        if (curResult == null
                                || dagStateRef.get() != DagState.RUNNING
                                || nodeStateMap.get(nodeResult.getNodeName()) != NodeState.RUNNING) {
                            return;
                        }
                        handleNodeExecuteResult(node, curResult);

                        // cur node execute succeeded, fire successor nodes
                        fireNextNode(node);
                    })
                    .exceptionally(e -> {
                        // Throw a DagEngineException if an unknown exception occurs during execution
                        log.error("Graph[{}] node[{}] execute encounter unknown exception ",
                                dagGraph.getGraphName(), node.getNodeName(), e);
                        dagDone(DagState.FAILED, new DagEngineException("unknown exception happened: " + e.getMessage(), e));
                        return null;
                    });
        }

        public void handleNodeExecuteResult(DagNode<?> node, NodeResultImpl<Object> nodeResult) {
            log.debug("Graph[{}] node[{}] execute state: {} result: {}",
                    dagGraph.getGraphName(), node.getNodeName(), nodeResult.getState(), nodeResult);

            dagContext.putNodeResult(nodeResult.getNodeName(), nodeResult);
            nodeStateMap.put(node.getNodeName(), nodeResult.getState());

            // not succeeded
            if (nodeResult.getState() != NodeState.SUCCEEDED) {
                dagDone(DagState.FAILED, nodeResult.getThrowable());
                return;
            }
        }


        public void executeNode(DagNode<?> node, NodeResultImpl<Object> nodeResult) {

            nodeResult.setStartTime(System.currentTimeMillis());
            nodeResult.setState(NodeState.RUNNING);

            try {
                NodeHandler<?> handler = node.getHandler();
                boolean evaluateResult = handler.evaluate(dagContext);

                if (evaluateResult) {
                    Object result = handler.execute(dagContext);
                    nodeResult.setResult(result);
                }

                nodeResult.setState(NodeState.SUCCEEDED);
            } catch (Exception e) {
                nodeResult.setThrowable(e);
                nodeResult.setState(NodeState.FAILED);
            }

            nodeResult.setEndTime(System.currentTimeMillis());
        }


        @Override
        public DagState getState() {
            return dagStateRef.get();
        }

        @Override
        public Map<String, NodeState> getNodeStateMap() {
            return new HashMap<>(nodeStateMap);
        }

        @Override
        public Throwable getThrowable() {
            return throwable;
        }

        @Override
        public Long getCostTime() {
            return endTime - startTime;
        }
    }


}
