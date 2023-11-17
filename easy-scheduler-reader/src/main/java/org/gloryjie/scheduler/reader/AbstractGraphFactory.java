package org.gloryjie.scheduler.reader;

import org.apache.commons.collections4.CollectionUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.DagGraphBuilder;
import org.gloryjie.scheduler.core.DefaultDagNode;
import org.gloryjie.scheduler.core.DefaultNodeHandler;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractGraphFactory implements DagGraphFactory {

    public static final String ANONYMOUS_HANDLER = "anonymous_handler";

    private final GraphDefinitionReader reader;


    public AbstractGraphFactory(GraphDefinitionReader reader) {
        this.reader = reader;
    }


    @Override
    public List<DagGraph> create(String graphDefinition) throws Exception {
        List<GraphDefinition> graphDefinitionList = reader.read(graphDefinition);
        return graphDefinitionList.stream().map(this::createDagGraph).collect(Collectors.toList());
    }


    protected DagGraph createDagGraph(GraphDefinition graphDefinition) {
        DagGraphBuilder dagGraphBuilder = new DagGraphBuilder().graphName(graphDefinition.getGraphName());

        List<DagNodeDefinition> nodeDefinitions = graphDefinition.getNodes();
        for (DagNodeDefinition nodeDefinition : nodeDefinitions) {
            DagNode<?> dagNode = createDagNode(nodeDefinition);
            dagGraphBuilder.addNode(dagNode);
        }

        return dagGraphBuilder.build();
    }


    protected DagNode<?> createDagNode(DagNodeDefinition nodeDefinition) {

        // get handler from factory if exists
        NodeHandler<Object> originalNodeHandler = Optional.ofNullable(nodeDefinition.getHandler())
                .map(this::getHandler).orElse(null);
        String handlerName = Optional.ofNullable(originalNodeHandler)
                .map(NodeHandler::handlerName).orElse(ANONYMOUS_HANDLER);

        // wrap handler predicate and expression
        Predicate<DagContext> predicate = createWhen(originalNodeHandler, nodeDefinition.getConditions());

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


    /**
     * Returns the handler with the given name.
     * @return The handler with the given name.
     */
    @Nullable
    protected abstract NodeHandler<Object> getHandler(String handlerName);


}
