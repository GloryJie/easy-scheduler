package org.gloryjie.scheduler.dynamic;

import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.reader.DagGraphConfigType;

public interface GraphRegistry {


    void registerGraph(DagGraph dagGraph);

    DagGraph getGraph(String name);

    default DagGraph getGraph(Class<?> clazz){
        return getGraph(clazz.getName());
    }

    void registerGraphClass(Class<?> clazz);

    void registerGraphConfig(DagGraphConfigType configType, String configuration);


}
