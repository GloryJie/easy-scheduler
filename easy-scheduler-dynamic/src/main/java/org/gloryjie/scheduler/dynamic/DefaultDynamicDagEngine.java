package org.gloryjie.scheduler.dynamic;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.api.ExecutorSelector;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.reader.DagGraphFactory;
import org.gloryjie.scheduler.reader.annotation.GraphClass;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandler;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandlerImpl;
import org.gloryjie.scheduler.reader.config.DagGraphConfigType;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultDynamicDagEngine extends ConcurrentDagEngine implements DynamicDagEngine {

    private DagGraphFactory dagGraphFactory;

    private final ConcurrentHashMap<String, DagGraph> graphMap = new ConcurrentHashMap<>();

    public DefaultDynamicDagEngine(DagGraphFactory dagGraphFactory) {
        super();
        this.dagGraphFactory = dagGraphFactory;
    }

    public DefaultDynamicDagEngine(ExecutorSelector executorSelector, DagGraphFactory dagGraphFactory) {
        super(executorSelector);
        this.dagGraphFactory = dagGraphFactory;
    }


    @Override
    public DagResult fireContext(Object context) {
        DagGraph dagGraph = createAndCacheGraph(context.getClass());
        return this.fire(dagGraph, context, dagGraph.timeout());
    }

    @Override
    public DagResult fireConfig(String configName, Object context) {
        DagGraph graph = this.getGraph(configName);
        if (graph == null){
            throw new DagEngineException("Could not find graph by name: " + configName);
        }
        return fire(graph, context);
    }

    @Override
    public void registerMethodHandler(Object bean) {
        Map<Method, MethodNodeHandler> methodMap = findMethodHandler(bean);
        if (MapUtils.isEmpty(methodMap)) {
            return;
        }

        for (Map.Entry<Method, MethodNodeHandler> entry : methodMap.entrySet()) {
            Method method = entry.getKey();
            MethodNodeHandler annotation = entry.getValue();

            MethodNodeHandlerImpl methodNodeHandler = new MethodNodeHandlerImpl(bean, method, annotation);
            this.registerHandler(methodNodeHandler);
        }
    }

    protected Map<Method, MethodNodeHandler> findMethodHandler(Object bean) {
        List<Method> methodList = MethodUtils.getMethodsListWithAnnotation(bean.getClass(), MethodNodeHandler.class);
        if (CollectionUtils.isEmpty(methodList)) {
            return Collections.emptyMap();
        }

        return methodList.stream().collect(Collectors.toMap(Function.identity(),
                item -> item.getAnnotation(MethodNodeHandler.class)));
    }

    @Override
    public DagGraphFactory getDagGraphFactory() {
        return this.dagGraphFactory;
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

    @Override
    public void registerGraph(DagGraph dagGraph) {
        Objects.requireNonNull(dagGraph, "DagGraph cannot be null");
        Objects.requireNonNull(dagGraph.getGraphName(), "DagGraph name cannot be null");
        this.graphMap.put(dagGraph.getGraphName(), dagGraph);
    }

    @Override
    public DagGraph getGraph(String name) {
        return this.graphMap.get(name);
    }

    @Override
    public void registerGraphClass(Class<?> clazz) {
        DagGraph graph = this.getGraph(clazz);
        if (graph != null){
            return;
        }
        createAndSaveGraph(clazz);
    }

    private synchronized void createAndSaveGraph(Class<?> clazz){
        DagGraph classGraph = dagGraphFactory.createClassGraph(clazz);
        this.registerGraph(classGraph);
    }


    @Override
    public void registerGraphConfig(DagGraphConfigType configType, String configuration) {
        List<DagGraph> configGraph = dagGraphFactory.createConfigGraph(configType, configuration);
        configGraph.forEach(this::registerGraph);
    }

    @Nullable
    @Override
    public NodeHandler<Object> getHandler(String handlerName) {
        return dagGraphFactory.getHandler(handlerName);
    }

    @Override
    public void registerHandler(NodeHandler<?> handler) {
        dagGraphFactory.registerHandler(handler);
    }
}
