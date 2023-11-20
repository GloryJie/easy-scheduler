package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;

import java.util.List;

public interface DagGraphFactory {


    List<DagGraph> createGraph(String graphDefinition) throws Exception;



}
