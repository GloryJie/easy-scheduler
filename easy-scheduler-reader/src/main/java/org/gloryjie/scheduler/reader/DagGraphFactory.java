package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;

import java.util.List;

public interface DagGraphFactory {


    List<DagGraph> createConfigGraph(String graphDefinition) throws Exception;


    DagGraph createClassGraph(Class<?> clzz) throws Exception;


}
