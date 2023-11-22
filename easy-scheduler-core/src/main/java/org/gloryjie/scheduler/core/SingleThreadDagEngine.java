package org.gloryjie.scheduler.core;

import lombok.Getter;
import lombok.Setter;
import org.gloryjie.scheduler.api.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SingleThreadDagEngine implements DagEngine {

    @Override
    public DagResult fire(DagGraph dagGraph, Object context, Long timeout) {
        DagExecutor dagExecutor = new DagExecutor(dagGraph, new MapDagContext(context), timeout);
        dagExecutor.start();
        return dagExecutor;
    }

    @Getter
    @Setter
    private static class NodeResultImpl<T> implements NodeResult<T> {

        private final String nodeName;
        private T result;
        private NodeState state;
        private Throwable throwable;
        private long startTime;
        private long endTime;

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

    @Slf4j
    private static class DagExecutor implements DagResult {

        private final DagGraph dagGraph;
        private final DagContext dagContext;
        private final Map<String, Integer> nodeInDegreeInfo;
        private final Long timeout;
        private DagState dagState = DagState.WAITING;
        Map<String, NodeState> nodeStateMap;
        private Throwable throwable;
        private long startTime;
        private long endTime;

        public DagExecutor(DagGraph dagGraph, DagContext dagContext, Long timeout) {
            this.dagGraph = dagGraph;
            this.dagContext = dagContext;
            this.timeout = timeout;
            nodeInDegreeInfo = new HashMap<>(dagGraph.getNodeInDegree());
            nodeStateMap = dagGraph.nodes().stream().map(DagNode::getNodeName)
                    .collect(Collectors.toMap(Function.identity(), nodeName -> NodeState.WAITING));
        }


        public void start() {
            startTime = System.currentTimeMillis();
            dagState = DagState.RUNNING;
            log.debug("Graph[{}] start", dagGraph.getGraphName());

            DagNode<?> node = dagGraph.getStartNode();
            fireNode(node);
            endTime = System.currentTimeMillis();
        }


        private void fireNode(DagNode<?> node) {
            checkTimeout();

            if (dagState != DagState.RUNNING) {
                return;
            }

            try {
                String nodeName = node.getNodeName();
                nodeStateMap.put(nodeName, NodeState.RUNNING);
                NodeResult<?> nodeResult = executeNode(node);

                throwable = nodeResult.getThrowable();
                nodeStateMap.put(nodeName, nodeResult.getState());
                dagContext.putNodeResult(nodeName, nodeResult);

                if (nodeResult.getState() != NodeState.SUCCEEDED) {
                    dagDone(DagState.FAILED);
                }
                log.debug("Graph[{}] node[{}] execute state: {} result: {}", dagGraph.getGraphName(),
                        nodeName, nodeResult.getState(), nodeResult.getResult());

            } catch (Exception e) {
                // Throw a DagEngineException if an unknown exception occurs during execution
                dagDone(DagState.FAILED);
                log.error("Graph[{}] node[{}] execute encount unknown exception ", dagGraph.getGraphName(), node.getNodeName(), e);
                throw new DagEngineException("Unknown exception happened: " + e.getMessage(), e.getCause());
            } finally {
                fireNextNode(node);
            }

        }

        private void fireNextNode(DagNode<?> curNode) {
            List<DagNode<?>> successorNodes = dagGraph.getSuccessorNodes(curNode.getNodeName());
            if (curNode == dagGraph.getEndNode() || CollectionUtils.isEmpty(successorNodes)) {
                dagDone(DagState.SUCCESS);
            } else {
                decrementSuccessorInDegree(successorNodes);
                checkAndFireSuccessorNodes(successorNodes);
            }
        }

        private void checkAndFireSuccessorNodes(List<DagNode<?>> successorNodes) {
            if (dagState != DagState.RUNNING) {
                return;
            }
            for (DagNode<?> successorNode : successorNodes) {
                String nodeName = successorNode.getNodeName();
                NodeState nodeState = nodeStateMap.get(nodeName);
                if (nodeState == NodeState.WAITING) {
                    int inDegree = nodeInDegreeInfo.get(nodeName);
                    if (inDegree == 0) {
                        fireNode(successorNode);
                    } else if (inDegree < 0) {
                        // safe check
                        throw new DagEngineException("inDegree could not be less than 0");
                    }
                }
            }
        }

        private void decrementSuccessorInDegree(List<DagNode<?>> successorNodes) {
            for (DagNode<?> successorNode : successorNodes) {
                String nodeName = successorNode.getNodeName();
                Integer inDegree = nodeInDegreeInfo.get(nodeName);
                nodeInDegreeInfo.put(nodeName, inDegree - 1);
            }
        }


        /**
         * Executes a given DAG node.
         *
         * @param node the DAG node to execute
         * @return the result of executing the node.
         */
        private NodeResult<?> executeNode(DagNode<?> node) {

            NodeHandler<?> curHandler = node.getHandler();
            NodeResultImpl<Object> nodeResult = new NodeResultImpl<>(node.getNodeName());
            nodeResult.setStartTime(System.currentTimeMillis());

            try {
                boolean evaluateResult = curHandler.evaluate(dagContext);
                log.debug("Graph[{}] node[{}] evaluate result: {}", dagGraph.getGraphName(), node.getNodeName(), evaluateResult);
                if (evaluateResult) {
                    Object result = curHandler.execute(dagContext);
                    nodeResult.setResult(result);
                    nodeResult.setState(NodeState.SUCCEEDED);
                }
            } catch (Exception e) {
                nodeResult.setThrowable(e);
                nodeResult.setState(NodeState.FAILED);
            }

            nodeResult.setEndTime(System.currentTimeMillis());
            return nodeResult;
        }


        private void checkTimeout() {
            long now = System.currentTimeMillis();
            if (timeout != null && timeout > 0 && (timeout < now - startTime)) {
                dagDone(DagState.TIMEOUT);
                throwable = new TimeoutException(String.format("Dag[graphName=%s] timeout expected: %s ms, cost: %s ms",
                        dagGraph.getGraphName(), timeout, now - startTime));
            }
        }

        private void dagDone(DagState state) {
            endTime = System.currentTimeMillis();
            dagState = state;
        }


        @Override
        public DagState getState() {
            return dagState;
        }

        @Override
        public Map<String, NodeState> getNodeStateMap() {
            return nodeStateMap;
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
