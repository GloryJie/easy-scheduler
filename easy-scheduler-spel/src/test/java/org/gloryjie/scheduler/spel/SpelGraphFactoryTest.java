package org.gloryjie.scheduler.spel;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.gloryjie.scheduler.api.*;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.reader.GraphDefinitionReader;
import org.gloryjie.scheduler.reader.JsonGraphDefinitionReader;
import org.gloryjie.scheduler.reader.YamlGraphDefinitionReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class SpelGraphFactoryTest {


    GraphDefinitionReader reader = new YamlGraphDefinitionReader();

    SpelGraphFactory spelGraphFactory = new SpelGraphFactory(reader);

    DagEngine dagEngine = new ConcurrentDagEngine();


    @ParameterizedTest
    @MethodSource("fileTypeAndReaderProvider")
    public void createSpelGraphTest(String fileType, SpelGraphFactory spelGraphFactory) throws Exception {
        File file = new File("src/test/resources/graph." + fileType);
        String cnt = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        List<DagGraph> graphList = spelGraphFactory.createGraph(cnt);

        assertFalse(graphList.isEmpty());
        assertNotNull(graphList.get(0));

        DagGraph dagGraph = graphList.get(0);
        assertNotNull(dagGraph.getGraphName());
        assertNotNull(dagGraph.getStartNode());
        assertNotNull(dagGraph.getEndNode());
        assertNotNull(dagGraph.nodes());

        DagNode<?> nodeA = dagGraph.getNode("A");
        assertNotNull(nodeA);
        assertEquals(500, nodeA.timeout());

        DagNode<?> nodeB = dagGraph.getNode("B");
        assertNotNull(nodeB);
        assertEquals(600, nodeB.timeout());
        assertEquals(Sets.newHashSet("A"), nodeB.dependNodeNames());


        DagNode<?> nodeC = dagGraph.getNode("C");
        assertNotNull(nodeC);
        assertEquals(700, nodeC.timeout());
        assertEquals(Sets.newHashSet("A","B"), nodeC.dependNodeNames());


        User userContext = createUserContext();
        DagResult dagResult = dagEngine.fire(dagGraph, userContext);

        assertTrue(dagResult.isDone());
        assertEquals(DagState.SUCCESS, dagResult.getState());

        assertEquals(20, userContext.getAge());
        assertEquals("male", userContext.getSex());
        assertEquals("Amy", userContext.getName());
    }


    static Stream<Arguments> fileTypeAndReaderProvider() {
        return Stream.of(
                Arguments.of("json", new SpelGraphFactory(new JsonGraphDefinitionReader())),
                Arguments.of("yml", new SpelGraphFactory(new YamlGraphDefinitionReader()))
        );
    }


    public User createUserContext(){
        User user = new User();
        user.setName("Jack");
        user.setAge(18);
        user.setUid(123);
        user.setSex("female");

        return user;
    }


}