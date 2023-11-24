package org.gloryjie.scheduler.reader;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.core.DagGraphBuilder;
import org.gloryjie.scheduler.core.DefaultDagNode;
import org.gloryjie.scheduler.core.DefaultNodeHandler;
import org.gloryjie.scheduler.reader.annotation.AnnotationDagGraphReader;
import org.gloryjie.scheduler.reader.annotation.GraphDefinitionClassReader;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandler;
import org.gloryjie.scheduler.reader.config.GraphDefinitionConfigReader;
import org.gloryjie.scheduler.reader.config.JsonGraphDefinitionReader;
import org.gloryjie.scheduler.reader.definition.DagNodeDefinition;
import org.gloryjie.scheduler.reader.definition.GraphDefinition;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractGraphFactory implements DagGraphFactory, HandlerRegistry {

    public static final String ANONYMOUS_HANDLER = "anonymous_handler";

    private final GraphDefinitionConfigReader configReader;

    private GraphDefinitionClassReader classReader;
    private final Map<String, NodeHandler<Object>> handlerMap = new ConcurrentHashMap<>();


    public AbstractGraphFactory() {
        this(new JsonGraphDefinitionReader(), new AnnotationDagGraphReader());
    }

    public AbstractGraphFactory(GraphDefinitionConfigReader configReader) {
        this(configReader, new AnnotationDagGraphReader());
    }


    public AbstractGraphFactory(GraphDefinitionConfigReader configReader, GraphDefinitionClassReader classReader) {
        this.configReader = configReader;
        this.classReader = classReader;
    }


    @Override
    public List<DagGraph> createConfigGraph(String graphDefinition) throws Exception {
        if (configReader == null) {
            throw new DagEngineException("Unsupported read text config");
        }
        List<GraphDefinition> graphDefinitionList = configReader.read(graphDefinition);
        return graphDefinitionList.stream().map(this::createDagGraph).collect(Collectors.toList());
    }

    @Override
    public DagGraph createClassGraph(Class<?> clzz) throws Exception {
        if (classReader == null) {
            throw new DagEngineException("Unsupported read class config");
        }
        GraphDefinition graphDefinition = classReader.read(clzz);
        return createDagGraph(graphDefinition);
    }

    protected DagGraph createDagGraph(GraphDefinition graphDefinition) {
        // TODO: 2023/11/20 check Graph definition

        DagGraphBuilder dagGraphBuilder = new DagGraphBuilder()
                .graphName(graphDefinition.getGraphName())
                .timeout(graphDefinition.getTimeout());

        List<DagNodeDefinition> nodeDefinitions = graphDefinition.getNodes();
        for (DagNodeDefinition nodeDefinition : nodeDefinitions) {
            DagNode<?> dagNode = createDagNode(nodeDefinition);
            dagGraphBuilder.addNode(dagNode);
        }

        return dagGraphBuilder.build();
    }


    protected DagNode<?> createDagNode(DagNodeDefinition nodeDefinition) {
        // get handler from factory if exists
        NodeHandler<Object> originalNodeHandler = null;
        if (StringUtils.isNotEmpty(nodeDefinition.getHandler())) {
            originalNodeHandler = this.getHandler(nodeDefinition.getHandler());
            if (originalNodeHandler == null) {
                throw new DagEngineException("Handler not found: " + nodeDefinition.getHandler());
            }
        }

        String handlerName = Optional.ofNullable(originalNodeHandler)
                .map(NodeHandler::handlerName).orElse(ANONYMOUS_HANDLER);

        // wrap handler predicate and expression
        Predicate<DagContext> predicate = createWhen(originalNodeHandler, nodeDefinition.getConditions());

        // wrap handler execution and expression
        Function<DagContext, Object> action = createAction(originalNodeHandler, nodeDefinition);

        // create new handler
        NodeHandler<Object> handler = DefaultNodeHandler.builder()
                .handlerName(handlerName)
                .timeout(nodeDefinition.getTimeout())
                .when(predicate)
                .action(action)
                .build();

        // create new dag node
        return DefaultDagNode.<Object>builder()
                .nodeName(nodeDefinition.getNodeName())
                .dependOn(nodeDefinition.getDependsOn().toArray(new String[0]))
                .timeout(nodeDefinition.getTimeout())
                .handler(handler)
                .build();
    }


    private Predicate<DagContext> createWhen(NodeHandler<Object> handler, List<String> conditions) {
        Predicate<DagContext> predicate = Optional.ofNullable(handler)
                .map(item -> (Predicate<DagContext>) item::evaluate)
                .orElse(null);

        Predicate<DagContext> expressionPredicate = null;
        if (CollectionUtils.isNotEmpty(conditions)) {
            predicate = conditions.stream()
                    .map(this::createCondition)
                    .reduce(predicate, (a, b)
                            -> a != null ? a.and(b) : b);
        }

        return predicate;
    }

    private Function<DagContext, Object> createAction(NodeHandler<Object> handler,
                                                      DagNodeDefinition nodeDefinition) {
        List<String> actions = nodeDefinition.getActions();

        // Create a consumer for each action and combine them into a single consumer
        Consumer<DagContext> expressConsumer = null;
        if (CollectionUtils.isNotEmpty(actions)) {
            expressConsumer = actions.stream()
                    .map(this::createConsumer)
                    .reduce(null, (a, b) -> a != null ? a.andThen(b) : b);
        }

        final Consumer<DagContext> mergeExpressConsumer = expressConsumer;

        // Create a function that execute the handler and expression
        final String fieldName = nodeDefinition.getRetFieldName();
        return dagContext -> {
            Object result = handler != null ? handler.execute(dagContext) : null;

            Object context = dagContext.getContext();
            try {
                // If the result is not null and the context is not null, set the field to context
                if (result != null && context != null && StringUtils.isNotEmpty(fieldName)) {
                    Field field = FieldUtils.getField(context.getClass(), fieldName, true);
                    if (field != null){
                        FieldUtils.writeField(field, context, result, true);
                    }
                }
            } catch (Exception e) {
                String msg = String.format("Failed to set field: %s to context: %s", fieldName,
                        context.getClass().getSimpleName());
                throw new DagEngineException(msg, e);
            }

            // execute other action
            if (mergeExpressConsumer != null) {
                mergeExpressConsumer.accept(dagContext);
            }

            return result;
        };
    }


    protected abstract Predicate<DagContext> createCondition(String condition);


    protected abstract Consumer<DagContext> createConsumer(String action);


    @Nullable
    @Override
    public NodeHandler<Object> getHandler(String handlerName) {
        Objects.requireNonNull(handlerName, "handler name cannot be null");
        return handlerMap.get(handlerName);
    }


    @Override
    public synchronized void registerHandler(NodeHandler handler) {
        Objects.requireNonNull(handler, "handler cannot be null");
        Objects.requireNonNull(handler.handlerName(), "handler name cannot be null");
        String handlerName = handler.handlerName();
        if (handlerMap.containsKey(handlerName)) {
            throw new DagEngineException(String.format("handler %s already exists", handlerName));
        }
        handlerMap.put(handlerName, handler);
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

            Predicate<DagContext> condition = null;
            if (ArrayUtils.isNotEmpty(annotation.conditions())){
                condition = Arrays.stream(annotation.conditions())
                        .map(this::createCondition).reduce(null, (a, b) -> a != null ? a.and(b) : b);
            }

            MethodNodeHandlerImpl methodNodeHandler = new MethodNodeHandlerImpl(bean, method, annotation, condition);
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

}
