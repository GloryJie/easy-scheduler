package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagGraph;

public interface GraphRegistry {


    void registerGraph(DagGraph dagGraph);



    DagGraph getGraph(String name);


}
