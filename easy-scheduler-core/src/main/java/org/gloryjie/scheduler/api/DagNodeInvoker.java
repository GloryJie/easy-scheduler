package org.gloryjie.scheduler.api;

public interface DagNodeInvoker {

    void invoke(DagNode node, DagContext dagContext);

}
