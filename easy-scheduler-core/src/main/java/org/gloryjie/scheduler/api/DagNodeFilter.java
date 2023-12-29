package org.gloryjie.scheduler.api;


public interface DagNodeFilter {


    Object invoke(DagNodeInvoker dagNodeInvoker, DagNode node, DagContext dagContext);


    default int getOrder() {
        return 0;
    }


}
