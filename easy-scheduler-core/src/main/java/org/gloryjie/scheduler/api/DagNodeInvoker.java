package org.gloryjie.scheduler.api;

public interface DagNodeInvoker {

    Object invoke(DagNode node, DagContext dagContext);

}
