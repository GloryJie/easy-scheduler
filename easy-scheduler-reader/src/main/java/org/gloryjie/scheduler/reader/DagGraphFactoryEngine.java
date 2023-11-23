package org.gloryjie.scheduler.reader;

import org.apache.commons.lang3.StringUtils;
import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.reader.annotation.GraphClass;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class DagGraphFactoryEngine implements MoreFireDagEngine, GraphRegistry {

    private DagGraphFactory dagGraphFactory;

    private DagEngine dagEngine;

    private ConcurrentHashMap<String, DagGraph> graphMap = new ConcurrentHashMap<>();


    @Override
    public DagResult fire(DagGraph dagGraph, Object context, Long timeout) {
        return dagEngine.fire(dagGraph, context, timeout);
    }


    @Override
    public void registerGraph(DagGraph dagGraph) {
        Objects.requireNonNull(dagGraph, "dagGraph cannot be null");
        graphMap.put(dagGraph.getGraphName(), dagGraph);
    }

    @Override
    public DagGraph getGraph(String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return graphMap.get(name);
    }

    @Override
    public DagResult fireWithContext(Object context) {
        DagGraph dagGraph = createAndCacheGraph(context.getClass());
        return dagEngine.fire(dagGraph, context, dagGraph.timeout());
    }

    @Override
    public DagResult fireWithConfig(String graphConfig) {
        return null;
    }

    @Override
    public DagResult fireWithGraphName(String graphName) {
        DagGraph dagGraph = getGraph(graphName);
        if (dagGraph == null){
            throw new DagEngineException("graph not found: " + graphName);
        }
        return dagEngine.fire(dagGraph, null, dagGraph.timeout());
    }


    private DagGraph createAndCacheGraph(Class<?> clzz) {
        GraphClass annotation = clzz.getAnnotation(GraphClass.class);
        if (annotation == null) {
            throw new DagEngineException("Graph class must be annotated with @GraphClass");
        }
        String graphName = annotation.graphName();
        if (StringUtils.isEmpty(graphName)) {
            graphName = clzz.getName();
        }

        DagGraph dagGraph = getGraph(graphName);
        if (dagGraph != null) {
            return dagGraph;
        }

        synchronized (this) {
            try {
                dagGraph = dagGraphFactory.createClassGraph(clzz);
                this.registerGraph(dagGraph);
                return dagGraph;
            } catch (DagEngineException e) {
                throw e;
            } catch (Exception e) {
                throw new DagEngineException("Failed to create dag graph from context", e);
            }
        }
    }
}
