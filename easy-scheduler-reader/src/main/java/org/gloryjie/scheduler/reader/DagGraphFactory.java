package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;

import java.util.List;

public interface DagGraphFactory {


    /**
     * Creates a list of DagGraph objects based on the provided graph definition.
     *
     * @param graphDefinition the graph definition
     * @return a list of DagGraph objects
     * @throws Exception if an error occurs during the process
     */
    default List<DagGraph> createConfigGraph(String graphDefinition) throws Exception{
        throw new UnsupportedOperationException();
    }


    default DagGraph createClassGraph(Class<?> clzz) throws Exception{
        throw new UnsupportedOperationException();
    }


}
