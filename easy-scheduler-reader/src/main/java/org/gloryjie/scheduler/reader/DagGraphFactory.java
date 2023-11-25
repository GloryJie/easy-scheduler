package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.core.DagEngineException;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface DagGraphFactory extends HandlerRegistry {


    /**
     * Creates a list of DagGraph objects based on the provided graph definition.
     *
     * @param configDefinition the graph definition
     * @return a list of DagGraph objects
     * @throws Exception if an error occurs during the process
     */
    List<DagGraph> createConfigGraph(DagGraphConfigType configType, String configDefinition);


    /**
     * Creates a DAG (Directed Acyclic Graph) graph for a given class.
     *
     * @param clzz The class for which the graph is created.
     * @return The created DAG graph.
     * @throws UnsupportedOperationException
     */
    DagGraph createClassGraph(Class<?> clzz);


    /**
     * Creates a predicate for the specified condition.
     *
     * @param condition the condition string
     * @return the condition Predicate
     */
    default Predicate<DagContext> createCondition(String condition) {
        throw new DagEngineException("Not support condition");
    }


    /**
     * Creates a consumer for the specified action.
     *
     * @param action the action for which to create a consumer
     * @return the consumer for the specified action
     */
    default Consumer<DagContext> createConsumer(String action) {
        throw new DagEngineException("Not support action");
    }


}
