package org.gloryjie.scheduler.reader;

import org.apache.commons.lang3.StringUtils;
import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.reader.annotation.GraphClass;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class DagGraphFactoryEngine implements DynamicFireDagEngine {

    private DagGraphFactory dagGraphFactory;

    private DagEngine dagEngine;

    private final ConcurrentHashMap<String, DagGraph> graphMap = new ConcurrentHashMap<>();

    public DagGraphFactoryEngine(DagGraphFactory dagGraphFactory, DagEngine dagEngine) {
        this.dagGraphFactory = dagGraphFactory;
        this.dagEngine = dagEngine;
    }

    @Override
    public DagResult fire(DagGraph dagGraph, Object context, Long timeout) {
        return dagEngine.fire(dagGraph, context, timeout);
    }


    @Override
    public DagResult fireContext(Object context) {
        DagGraph dagGraph = createAndCacheGraph(context.getClass());
        return dagEngine.fire(dagGraph, context, dagGraph.timeout());
    }

    /**
     * Creates and caches a DagGraph based on the provided class.
     * If the DagGraph already exists in the cache, it is returned directly.
     *
     * @param clzz the class used to create the DagGraph
     * @return the created or cached DagGraph
     * @throws DagEngineException if the DagGraph cannot be created
     */
    private DagGraph createAndCacheGraph(Class<?> clzz) {

        // Check if the DagGraph already exists in the cache
        DagGraph dagGraph = getGraphByClass(clzz);
        if (dagGraph != null) {
            return dagGraph;
        }

        // If not exists in cache, try to create and cache
        synchronized (this) {
            try {
                dagGraph = dagGraphFactory.createClassGraph(clzz);
                this.graphMap.put(dagGraph.getGraphName(), dagGraph);
                return dagGraph;
            } catch (DagEngineException e) {
                throw e;
            } catch (Exception e) {
                throw new DagEngineException("Failed to create dag graph from context", e);
            }
        }
    }


    private DagGraph getGraphByClass(Class<?> clzz) {
        GraphClass annotation = clzz.getAnnotation(GraphClass.class);
        if (annotation == null) {
            throw new DagEngineException("Graph class must be annotated with @GraphClass");
        }
        String graphName = annotation.graphName();
        if (StringUtils.isEmpty(graphName)) {
            graphName = clzz.getName();
        }
        return this.graphMap.get(graphName);
    }
}
