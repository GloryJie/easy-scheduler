package org.gloryjie.scheduler.core;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class GuavaGraphTest {

    @Test
    public void directedGraphTest() throws Exception{

        MutableGraph<String> mutableGraph = GraphBuilder.directed().allowsSelfLoops(false).build();

        mutableGraph.addNode("A");
        mutableGraph.addNode("B");
        mutableGraph.addNode("C");

        mutableGraph.putEdge("A", "B");
        mutableGraph.putEdge("A", "C");
        mutableGraph.putEdge("B", "C");


        System.out.println(mutableGraph.inDegree("A"));
        System.out.println(mutableGraph.inDegree("B"));
        System.out.println(mutableGraph.inDegree("C"));

        System.out.println(mutableGraph.toString());


        System.out.println("isDirected: " + mutableGraph.isDirected());
        System.out.println("isSelfLoop: " + mutableGraph.allowsSelfLoops());
        System.out.println("isCyclic: " + Graphs.hasCycle(mutableGraph));
    }


}
