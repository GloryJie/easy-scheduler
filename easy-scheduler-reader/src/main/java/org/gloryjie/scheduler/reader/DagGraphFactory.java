package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;

public interface DagGraphFactory {


    DagGraph create(String graphDefinition);



}
