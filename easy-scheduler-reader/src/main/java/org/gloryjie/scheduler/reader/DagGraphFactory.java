package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.reader.definition.GraphDefinition;

import java.util.List;

public interface DagGraphFactory {


    List<DagGraph> createGraph(String graphDefinition) throws Exception;


    DagGraph createGraph(Class<?> clzz) throws Exception;


}
