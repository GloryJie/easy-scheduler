package org.gloryjie.scheduler.reader;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.core.DagGraphBuilder;
import org.gloryjie.scheduler.core.DefaultDagNode;
import org.gloryjie.scheduler.core.DefaultNodeHandler;
import org.gloryjie.scheduler.reader.annotation.AnnotationDagGraphReader;
import org.gloryjie.scheduler.reader.config.JsonGraphDefinitionReader;
import org.gloryjie.scheduler.reader.definition.DagNodeDefinition;
import org.gloryjie.scheduler.reader.definition.GraphDefinition;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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



    public AbstractGraphFactory(){
        this(new JsonGraphDefinitionReader(), new AnnotationDagGraphReader());
    }

    public AbstractGraphFactory(GraphDefinitionConfigReader configReader){
        this(configReader, new AnnotationDagGraphReader());
    }


    public AbstractGraphFactory(GraphDefinitionConfigReader configReader, GraphDefinitionClassReader classReader) {
        this.configReader = configReader;
        this.classReader = classReader;
    }


    @Override
    public List<DagGraph> createGraph(String graphDefinition) throws Exception {
        if (configReader == null){
            throw new DagEngineException("Unsupported read text config");
        }
        List<GraphDefinition> graphDefinitionList = configReader.read(graphDefinition);
        return graphDefinitionList.stream().map(this::createDagGraph).collect(Collectors.toList());
    }

    @Override
    public DagGraph createGraph(Class<?> clzz) throws Exception {
        if (classReader == null){
            throw new DagEngineException("Unsupported read class config");
        }
        GraphDefinition graphDefinition = classReader.read(clzz);
        return createDagGraph(graphDefinition);
    }

    protected DagGraph createDagGraph(GraphDefinition graphDefinition) {
        // TODO: 2023/11/20 check Graph definition

        DagGraphBuilder dagGraphBuilder = new DagGraphBuilder().graphName(graphDefinition.getGraphName());

        Class<?> contextClass = null;
        if (StringUtils.isNotEmpty(graphDefinition.getContextClass())) {
            try {
                contextClass = Class.forName(graphDefinition.getContextClass());
            } catch (Exception e) {
                throw new DagEngineException("Failed to load context class: " + graphDefinition.getContextClass(), e);
            }
        }

        List<DagNodeDefinition> nodeDefinitions = graphDefinition.getNodes();
        for (DagNodeDefinition nodeDefinition : nodeDefinitions) {
            DagNode<?> dagNode = createDagNode(contextClass, nodeDefinition);
            dagGraphBuilder.addNode(dagNode);
        }

        return dagGraphBuilder.build();
    }


    protected DagNode<?> createDagNode(Class<?> contextClass, DagNodeDefinition nodeDefinition) {
        // get handler from factory if exists
        NodeHandler<Object> originalNodeHandler = null;
        if (StringUtils.isNotEmpty(nodeDefinition.getHandler())){
            originalNodeHandler = this.getHandler(nodeDefinition.getHandler());
            if (originalNodeHandler == null){
                throw new DagEngineException("Handler not found: " + nodeDefinition.getHandler());
            }
        }

        String handlerName = Optional.ofNullable(originalNodeHandler)
                .map(NodeHandler::handlerName).orElse(ANONYMOUS_HANDLER);

        // wrap handler predicate and expression
        Predicate<DagContext> predicate = createWhen(originalNodeHandler, nodeDefinition.getConditions());

        originalNodeHandler = wrapHandlerCouldSetRetField(contextClass, nodeDefinition, originalNodeHandler);

        // wrap handler execution and expression
        Function<DagContext, Object> action = createAction(originalNodeHandler, nodeDefinition.getActions());

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


    private NodeHandler<Object> wrapHandlerCouldSetRetField(Class<?> contextClass,
                                                        DagNodeDefinition nodeDefinition,
                                                        NodeHandler<Object> nodeHandler){
        // set field value if appoint field name
        if (contextClass != null && nodeHandler != null
                && StringUtils.isNotEmpty(nodeDefinition.getRetFieldName())){
            Field field = FieldUtils.getField(contextClass, nodeDefinition.getRetFieldName(), true);
            if (field == null) {
                throw new DagEngineException(String.format("field %s not found in class %s",
                        nodeDefinition.getRetFieldName(), contextClass.getName()));
            }

            return new ContextSetFieldHandler(nodeHandler, field);
        }
        return nodeHandler;
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

    private Function<DagContext, Object> createAction(NodeHandler<Object> handler, List<String> actions) {

        // expression to consumer
        Consumer<DagContext> expressConsumer = null;
        if (CollectionUtils.isNotEmpty(actions)) {
            expressConsumer = actions.stream()
                    .map(this::createConsumer)
                    .reduce(expressConsumer, (a, b) -> a != null ? a.andThen(b) : b);
        }

        final Consumer<DagContext> mergeExpressConsumer = expressConsumer;

        // create a function that execute the handler and expression
        return dagContext -> {
            Object result = handler != null ? handler.execute(dagContext) : null;
            // execute other action
            if (mergeExpressConsumer != null) {
                mergeExpressConsumer.accept(dagContext);
            }

            return result;
        };
    }


    @Nullable
    protected abstract Predicate<DagContext> createCondition(String condition);


    @Nullable
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
}
