package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.DependencyType;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("all")
public class BaseGraph {


    public static DagGraph buidGraph(Map<String, NodeHandler> actionMap,
                                     Map<String, Map<String, DependencyType>> dependMap) {
        actionMap = actionMap == null ? new HashMap<>() : actionMap;
        dependMap = dependMap == null ? new HashMap<>() : dependMap;
        Map<String, DefaultDagNode.Builder> builderMap = new HashMap<>();
        DefaultDagNode.Builder<Object> nodeBuilderA = DefaultDagNode.builder()
                .nodeName("A")
                .handler((dagNode, dagContext) -> {
                    return "A";
                });
        builderMap.put("A", nodeBuilderA);

        DefaultDagNode.Builder<Object> nodeBuilderB = DefaultDagNode.builder()
                .nodeName("B")
                .handler((dagNode, dagContext) -> {
                    return "B";
                });
        builderMap.put("B", nodeBuilderB);

        DefaultDagNode.Builder<Object> nodeBuilderC = DefaultDagNode.builder()
                .nodeName("C")
                .handler((dagNode, dagContext) -> {
                    return "C";
                });
        builderMap.put("C", nodeBuilderC);


        DefaultDagNode.Builder<Object> nodeBuilderD = DefaultDagNode.builder()
                .nodeName("D")
                .handler((dagNode, dagContext) -> {
                    return "D";
                })
                .dependOn("A", "B");
        builderMap.put("D", nodeBuilderD);


        DefaultDagNode.Builder<Object> nodeBuilderE = DefaultDagNode.builder()
                .nodeName("E")
                .handler((dagNode, dagContext) -> {
                    return "E";
                })
                .dependOn("B", "C");
        builderMap.put("E", nodeBuilderE);

        DefaultDagNode.Builder<Object> nodeBuilderF = DefaultDagNode.builder()
                .nodeName("F")
                .handler((dagNode, dagContext) -> {
                    return "F";
                }).dependOn("D");
        builderMap.put("F", nodeBuilderF);

        DefaultDagNode.Builder<Object> nodeBuilderG = DefaultDagNode.builder()
                .nodeName("G")
                .handler((dagNode, dagContext) -> {
                    return "G";
                }).dependOn("D", "E");
        builderMap.put("G", nodeBuilderG);

        DefaultDagNode.Builder<Object> nodeBuilderH = DefaultDagNode.builder()
                .nodeName("H")
                .handler((dagNode, dagContext) -> {
                    return "H";
                }).dependOn("C");
        builderMap.put("H", nodeBuilderH);

        List<DagNode> nodeList = new ArrayList<>();
        for (Map.Entry<String, DefaultDagNode.Builder> entry : builderMap.entrySet()) {
            String name = entry.getKey();
            DefaultDagNode.Builder builder = entry.getValue();
            if (actionMap.containsKey(name)) {
                builder.handler((NodeHandler) actionMap.get(name));
            }

            if (dependMap.containsKey(name)) {
                for (Map.Entry<String, DependencyType> dependEntry : dependMap.get(name).entrySet()) {
                    builder.dependOn(dependEntry.getValue(), dependEntry.getKey());
                }
            }
            builder.timeout(100l);
            nodeList.add(builder.build());
        }

        return new DagGraphBuilder()
                .graphName("baseGraphTest")
                .addNodes(nodeList.toArray(new DagNode[0]))
                .timeout(200l)
                .build();
    }


}
