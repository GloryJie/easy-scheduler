package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;

import java.util.List;

public interface DagGraphFactory {


    List<DagGraph> create(String graphDefinition) throws Exception;



}
