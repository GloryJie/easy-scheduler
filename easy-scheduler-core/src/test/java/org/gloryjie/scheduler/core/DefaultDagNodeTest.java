package org.gloryjie.scheduler.core;

import org.gloryjie.scheduler.api.DagNode;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DefaultDagNodeTest {


    @Test
    public void createDefaultDagNodeTest(){
        DagNode<String> dagNode = DefaultDagNode.<String>builder()
                .nodeName("A")
                .handler(new DefaultNodeHandler<>("A", null, null, null))
                .dependOn("B", "C", "D")
                .build();

        assertNotNull(dagNode.getNodeName());
        assertNotNull(dagNode.getHandler());
        assertEquals(Sets.newHashSet("B", "C", "D"), dagNode.dependNodeNames());

        dagNode.removeDependency("D");
        assertEquals(Sets.newHashSet("B", "C"), dagNode.dependNodeNames());

        dagNode.addDependency("D");
        assertEquals(Sets.newHashSet("B", "C", "D"), dagNode.dependNodeNames());
    }


}