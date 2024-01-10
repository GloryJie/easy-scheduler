package org.gloryjie.scheduler.core;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.gloryjie.scheduler.api.*;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

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

    private final List<DagNodeFilter> dagNodeFilters = new CopyOnWriteArrayList<>();

    private DagNodeInvoker dagNodeInvoker;

    private DagNodeInvoker rawDagNodeInvoker;


    public ConcurrentDagEngine() {
        this(new SingleExcutorSelector(Runtime.getRuntime().availableProcessors()));
    }

    public ConcurrentDagEngine(ExecutorSelector executorSelector) {
        this.executorSelector = executorSelector;
        this.rawDagNodeInvoker = new RawDagNodeInvoker();
        this.dagNodeInvoker = rawDagNodeInvoker;
    }


    @Override
    public DagResult fire(DagGraph dagGraph, Object context, Long timeout) {
        ExecutorService executorService = null;
        if (executorSelector != null) {
            executorService = executorSelector.select(dagGraph.getGraphName());
        }
        ConcurrentDagContext dagContext = new ConcurrentDagContext(context);
        DagExecutor dagExecutor = new DagExecutor(executorService, dagGraph, dagContext, timeout);
        dagExecutor.start();
        return dagExecutor;
    }

    @Override
    public synchronized void registerFilter(DagNodeFilter filter) {
        dagNodeFilters.add(filter);
        dagNodeFilters.sort(Comparator.comparing(DagNodeFilter::getOrder));
        DagNodeInvoker nodeInvoker = rawDagNodeInvoker;

        for (DagNodeFilter dagNodeFilter : dagNodeFilters) {
            nodeInvoker = new DagNodeFilterInvoker(nodeInvoker, dagNodeFilter);
        }
        this.dagNodeInvoker = nodeInvoker;
    }


    /**
     * Wraps the given CompletableFuture with a timeout.
     *
     * @param dagNode    the DagNode associated with the CompletableFuture
     * @param nodeResult the result of the DagNode
     * @param future     the CompletableFuture to wrap with a timeout
     * @return a CompletableFuture that completes with the result of the original CompletableFuture,
     * or completes with nodeResult that state is TIMEOUT and throwable is TimeoutException
     * if the original does not complete within the specified timeout period
     */
    protected CompletableFuture<NodeResultImpl<Object>> wrapNodeFutureWithTimeout(
            DagNode<?> dagNode,
            NodeResultImpl<Object> nodeResult,
            CompletableFuture<NodeResultImpl<Object>> future
    ) {
        if (dagNode.timeout() == null || dagNode.timeout() <= 0 || future.isDone()) {
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
    @ToString
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


    @ToString(onlyExplicitlyIncluded = true)
    private class DagExecutor implements DagResult {

        private final ExecutorService executorService;
        private final DagGraph dagGraph;
        @ToString.Include
        private final DagContext dagContext;
        @ToString.Include
        private final Map<String, AtomicInteger> nodeInDegreeInfo;
        @ToString.Include
        private final Long timeout;


        /**
         * Fields that change at run time
         */
        private final CountDownLatch countDownLatch;
        private final AtomicReference<DagState> dagStateRef = new AtomicReference<>(DagState.WAITING);
        @ToString.Include
        private final ConcurrentHashMap<String, NodeState> nodeStateMap;
        @ToString.Include
        private volatile Throwable throwable;
        @ToString.Include
        private volatile long startTime;
        @ToString.Include
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

            // Wait for the DAG execution to complete
            try {
                if (timeout == null || timeout <= 0) {
                    countDownLatch.await();
                } else {
                    boolean awaitResult = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                    if (!awaitResult) {
                        String msg = String.format("Graph[%s] not completed in %s ms",
                                dagGraph.getGraphName(), timeout);
                        log.debug(msg);
                        dagDone(DagState.TIMEOUT, new TimeoutException(msg));
                    }
                }
            } catch (InterruptedException e) {
                log.debug("Graph[{}] interrupted", dagGraph.getGraphName());
                dagDone(DagState.INTERRUPTED, e);
            }
        }

        private void fireNextNode(DagNode<?> curNode, NodeResultImpl<Object> curNodeResult) {
            if (dagStateRef.get() != DagState.RUNNING) {
                return;
            }

            // Mark the entire graph as failed if the node execution was not successful and there are strong dependencies
            if (curNodeResult.getState() != NodeState.SUCCEEDED) {
                Map<String, DependencyType> successorNodeTypes =
                        dagGraph.getSuccessorNodeTypes(curNode.getNodeName());
                boolean hadStrongDepend = successorNodeTypes.values().stream()
                        .anyMatch(type -> type == DependencyType.STRONG);
                // If there are strong dependencies, mark the DAG as failed
                if (hadStrongDepend) {
                    dagDone(DagState.FAILED, curNodeResult.getThrowable());
                    return;
                }
            }

            List<DagNode<?>> successorNodes = dagGraph.getSuccessorNodes(curNode.getNodeName());
            // If the current node is the end node or there are no successor nodes, mark the DAG as succeeded
            if (curNode == dagGraph.getEndNode() || CollectionUtils.isEmpty(successorNodes)) {
                dagDone(DagState.SUCCEED, null);
            } else {
                // Decrement the in-degree of successor nodes and fire them
                decrementIndegreeAndFireSuccessorNodes(successorNodes);
            }
        }

        private void decrementIndegreeAndFireSuccessorNodes(List<DagNode<?>> successorNodes) {
            if (dagStateRef.get() != DagState.RUNNING) {
                return;
            }

            for (DagNode<?> successorNode : successorNodes) {
                String nodeName = successorNode.getNodeName();
                NodeState nodeState = nodeStateMap.get(nodeName);
                // Check if the node is in the wai ting state and the in-degree is 0
                if (nodeState == NodeState.WAITING) {
                    int inDegree = nodeInDegreeInfo.get(successorNode.getNodeName()).decrementAndGet();
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
            // Update the state, throwable, and endTime only if the current state is RUNNING
            if (this.dagStateRef.compareAndSet(DagState.RUNNING, state)) {
                this.throwable = throwable;
                this.endTime = System.currentTimeMillis();
            }

            // Ignore the result of compareAndSet and wake up the invoking thread
            this.countDownLatch.countDown();
        }

        private void fireNode(DagNode<?> node) {

            CompletableFuture<NodeResultImpl<Object>> nodeFuture = getNodeExecuteFuture(node);

            nodeFuture.thenAccept(curResult -> {
                        // Check the DAG and node state before executing
                        if (curResult == null
                                || dagStateRef.get() != DagState.RUNNING
                                || nodeStateMap.get(curResult.getNodeName()) != NodeState.RUNNING) {
                            return;
                        }
                        // handle cur node execute result
                        handleNodeExecuteResult(node, curResult);

                        // check timeout
                        checkGraphExecuteTimeout();

                        // fire successor nodes
                        fireNextNode(node, curResult);
                    })
                    .exceptionally(e -> {
                        // Throw a DagEngineException if an unknown exception occurs during execution
                        log.error("Graph[{}] node[{}] execute encounter unknown exception ",
                                dagGraph.getGraphName(), node.getNodeName(), e);
                        dagDone(DagState.FAILED, new DagEngineException("unknown exception happened: " + e.getMessage(), e));
                        return null;
                    });
        }

        private void checkGraphExecuteTimeout() {
            long now = System.currentTimeMillis();
            if (timeout != null && timeout > 0 && (timeout < now - startTime)) {
                TimeoutException timeoutException = new TimeoutException(
                        String.format("Dag[graphName=%s] timeout expected: %s ms, cost: %s ms",
                                dagGraph.getGraphName(), timeout, now - startTime));
                dagDone(DagState.TIMEOUT, timeoutException);
            }
        }


        private CompletableFuture<NodeResultImpl<Object>> getNodeExecuteFuture(DagNode node) {
            CompletableFuture<NodeResultImpl<Object>> nodeFuture = null;

            NodeResultImpl<Object> nodeResult = new NodeResultImpl<>(node.getNodeName());
            nodeResult.setSubmitTime(System.currentTimeMillis());

            Supplier<NodeResultImpl<Object>> supplier = nodeExecuteResultSupplier(node, nodeResult);

            // Execute the node
            if (node.getHandler() == null || executorService == null) {
                nodeFuture = CompletableFuture.completedFuture(supplier.get());
            } else {
                nodeFuture = CompletableFuture.supplyAsync(supplier, executorService);
                // Submit the node timeout future
                nodeFuture = wrapNodeFutureWithTimeout(node, nodeResult, nodeFuture);
            }
            return nodeFuture;
        }

        @SuppressWarnings("all")
        private Supplier<NodeResultImpl<Object>> nodeExecuteResultSupplier(DagNode node, NodeResultImpl nodeResult) {
            return () -> {
                // Check the DAG and node state before executing
                if (dagStateRef.get() != DagState.RUNNING
                        || nodeStateMap.get(node.getNodeName()) != NodeState.WAITING) {
                    return null;
                }
                nodeStateMap.put(node.getNodeName(), NodeState.RUNNING);

                executeNode(node, nodeResult);

                return nodeResult;
            };
        }

        public void handleNodeExecuteResult(DagNode<?> node, NodeResultImpl<Object> nodeResult) {
            log.debug("Graph[{}] node[{}] execute state: {} result: {}",
                    dagGraph.getGraphName(), node.getNodeName(), nodeResult.getState(), nodeResult);

            dagContext.putNodeResult(nodeResult.getNodeName(), nodeResult);
            nodeStateMap.put(node.getNodeName(), nodeResult.getState());
        }


        @SuppressWarnings("all")
        public void executeNode(DagNode node, NodeResultImpl<Object> nodeResult) {
            // init node
            nodeResult.setStartTime(System.currentTimeMillis());
            nodeResult.setState(NodeState.RUNNING);

            DagNodeInvoker invoker = dagNodeInvoker;
            try {
                Object result = invoker.invoke(node, dagContext);
                nodeResult.setResult(result);
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


    @SuppressWarnings({"all"})
    static class RawDagNodeInvoker implements DagNodeInvoker {

        @Override
        public Object invoke(DagNode node, DagContext dagContext) {
            NodeHandler handler = node.getHandler();
            Object result = null;
            if (handler != null) {
                boolean evaluateResult = handler.evaluate(node, dagContext);
                if (evaluateResult) {
                    result = handler.execute(node, dagContext);
                }
            }
            return result;
        }

    }

    @SuppressWarnings({"raw"})
    static class DagNodeFilterInvoker implements DagNodeInvoker {

        private final DagNodeInvoker dagNodeInvoker;

        private final DagNodeFilter dagNodeFilter;

        public DagNodeFilterInvoker(DagNodeInvoker dagNodeInvoker, DagNodeFilter dagNodeFilter) {
            this.dagNodeInvoker = dagNodeInvoker;
            this.dagNodeFilter = dagNodeFilter;
        }

        @Override
        public Object invoke(DagNode node, DagContext dagContext) {
            return dagNodeFilter.invoke(dagNodeInvoker, node, dagContext);
        }
    }


}
