package org.gloryjie.scheduler.example.hello;

import org.gloryjie.scheduler.api.*;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.DagGraphBuilder;
import org.gloryjie.scheduler.core.DefaultDagNode;
import org.gloryjie.scheduler.core.DefaultNodeHandler;

public class HelloLauncher {


    public static void main(String[] args) {

        NodeHandler printHandler = DefaultNodeHandler.builder()
                .handlerName("A")
                .when(context -> context.getContext() != null)
                .action((dagNode, dagContext) -> {
                    System.out.println("Hello: " + dagNode.getNodeName());
                    return null;
                }).build();

        DagNode dagNodeA = DefaultDagNode.builder().nodeName("A")
                .handler(printHandler).build();

        DagNode dagNodeB = DefaultDagNode.builder().nodeName("B")
                .handler(printHandler).dependOn("A").build();

        DagGraph dagGraph = new DagGraphBuilder()
                .graphName("helloGraph")
                .addNodes(dagNodeA, dagNodeB)
                .build();

        DagEngine dagEngine = new ConcurrentDagEngine();
        DagResult dagResult = dagEngine.fire(dagGraph, "your context");
        if (dagResult.getState() == DagState.SUCCEED) {
            // do something
        } else {
            Throwable err = dagResult.getThrowable();
            err.printStackTrace();
        }

    }

}
