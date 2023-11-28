package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.reader.config.DagGraphConfigType;

import java.util.List;

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


}
