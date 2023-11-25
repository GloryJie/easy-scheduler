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

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractGraphFactory implements DagGraphFactory {

    public static final String ANONYMOUS_HANDLER = "anonymous_handler";

    private final DagGraphReader graphReader;

    private final ConcurrentHashMap<String, NodeHandler<Object>> handlerMap = new ConcurrentHashMap<>();

    public AbstractGraphFactory() {
        this(new CompositeDagGraphReader());
    }

    public AbstractGraphFactory(DagGraphReader graphReader) {
        this.graphReader = graphReader;
    }


    @Override
    public List<DagGraph> createConfigGraph(DagGraphConfigType configType, String configDefinition) {
        List<GraphDefinition> definitionList = graphReader.readFromConfig(configType, configDefinition);
        return definitionList.stream().map(this::createDagGraph).collect(Collectors.toList());
    }

    @Override
    public DagGraph createClassGraph(Class<?> clzz) {
        GraphDefinition definition = graphReader.readFromClass(clzz);
        return createDagGraph(definition);
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
        // Get the original node handler from the handler registry if it exists
        NodeHandler<Object> originalNodeHandler = null;
        if (StringUtils.isNotEmpty(nodeDefinition.getHandler())) {
            originalNodeHandler = this.getHandler(nodeDefinition.getHandler());
            if (originalNodeHandler == null) {
                throw new DagEngineException("Handler not found: " + nodeDefinition.getHandler());
            }
        }

        // Get the handler name or use the anonymous handler name if it is null
        String handlerName = Optional.ofNullable(originalNodeHandler)
                .map(NodeHandler::handlerName).orElse(ANONYMOUS_HANDLER);

        // Create the predicate for the handler
        Predicate<DagContext> predicate = createWhen(originalNodeHandler, nodeDefinition.getConditions());

        // Create the action for the handler
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


    /**
     * Creates a predicate based on the handler and conditions.
     *
     * @param handler    The node handler
     * @param conditions The list of conditions
     * @return The created predicate
     */
    private Predicate<DagContext> createWhen(NodeHandler<Object> handler, List<String> conditions) {
        // Create initial predicate based on the handler
        Predicate<DagContext> predicate = Optional.ofNullable(handler)
                .map(item -> (Predicate<DagContext>) item::evaluate)
                .orElse(null);

        // Combine the initial predicate with the conditions, if any
        if (CollectionUtils.isNotEmpty(conditions)) {
            predicate = conditions.stream()
                    .map(this::createCondition)
                    .reduce(predicate, (a, b) -> a != null ? a.and(b) : b);
        }

        return predicate;
    }

    /**
     * Creates a function that executes the handler and expression.
     *
     * @param handler        The node handler
     * @param nodeDefinition The node definition
     * @return The function that executes the handler and expression
     */
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
            // If the result is not null, set the field to context
            if (result != null && StringUtils.isNotEmpty(fieldName)) {
                try {
                    // Get the field and set the value if it exists, otherwise put the value in dagContext
                    if (context != null) {
                        Field field = FieldUtils.getField(context.getClass(), fieldName, true);
                        if (field != null && field.getType().isAssignableFrom(result.getClass())) {
                            FieldUtils.writeField(field, context, result, true);
                        } else {
                            dagContext.put(fieldName, result);
                        }
                    } else {
                        dagContext.put(fieldName, result);
                    }
                } catch (Exception e) {
                    throw new DagEngineException(String.format("Failed to set field[%s] to context", fieldName), e);
                }
            }
            // Execute other actions
            if (mergeExpressConsumer != null) {
                mergeExpressConsumer.accept(dagContext);
            }

            return result;
        };
    }

    @Nullable
    @Override
    public NodeHandler<Object> getHandler(String handlerName) {
        return handlerMap.get(handlerName);
    }


    @SuppressWarnings("unchecked")
    @Override
    public synchronized void registerHandler(NodeHandler<?> handler) {
        Objects.requireNonNull(handler, "Register handler cannot be null");
        Objects.requireNonNull(handler.handlerName(), "Register handler name cannot be null");
        if (getHandler(handler.handlerName()) != null) {
            throw new DagEngineException(String.format("Handler name[%s] already exists", handler.handlerName()));
        }
        handlerMap.put(handler.handlerName(), (NodeHandler<Object>) handler);
    }


    /**
     * Creates a predicate for the specified condition.
     *
     * @param condition the condition string
     * @return the condition Predicate
     */
    protected Predicate<DagContext> createCondition(String condition) {
        throw new DagEngineException("Not support use condition");
    }


    /**
     * Creates a consumer for the specified action.
     *
     * @param action the action for which to create a consumer
     * @return the consumer for the specified action
     */
    protected Consumer<DagContext> createConsumer(String action) {
        throw new DagEngineException("Not support use action");
    }

}
