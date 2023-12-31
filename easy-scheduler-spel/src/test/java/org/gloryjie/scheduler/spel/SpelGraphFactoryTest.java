package org.gloryjie.scheduler.spel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.gloryjie.scheduler.api.*;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.DefaultNodeHandler;
import org.gloryjie.scheduler.reader.config.DagGraphConfigType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class SpelGraphFactoryTest {


    private static DagEngine dagEngine = new ConcurrentDagEngine();

    @ParameterizedTest
    @MethodSource("fileTypeAndReaderProvider")
    public void createSpelGraphTest(DagGraphConfigType configType, SpelGraphFactory spelGraphFactory) throws Exception {
        File file = new File("src/test/resources/graph." + configType.name().toLowerCase());
        String cnt = FileUtils.readFileToString(file, StandardCharsets.UTF_8);


        List<DagGraph> graphList = spelGraphFactory.createConfigGraph(configType, cnt);

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
        assertEquals(Sets.newHashSet(), nodeB.dependNodeNames());


        DagNode<?> nodeC = dagGraph.getNode("C");
        assertNotNull(nodeC);
        assertEquals(700, nodeC.timeout());
        assertEquals(Sets.newHashSet("B"), nodeC.dependNodeNames());


        UserInfoContext userInfoContext = createUserContext();
        DagResult dagResult = dagEngine.fire(dagGraph, userInfoContext);

        assertTrue(dagResult.isDone());
        assertEquals(DagState.SUCCEED, dagResult.getState());

        // assert result
        assertEquals("Jack", userInfoContext.getUserInfo().getName());
        assertEquals(22, userInfoContext.getUserInfo().getAge());
        assertEquals("Shenzhen", userInfoContext.getUserInfo().getAddress());

        // dagNode D have action which add python to courseList
        assertEquals(Lists.newArrayList("Math", "Java", "Go", "Rust", "Python"), userInfoContext.getCourseList());

        assertEquals("Math", userInfoContext.getCourseScoreList().get(0).getCourseName());
        assertEquals(60, userInfoContext.getCourseScoreList().get(0).getScore());

        assertEquals("Java", userInfoContext.getCourseScoreList().get(1).getCourseName());
        assertEquals(70, userInfoContext.getCourseScoreList().get(1).getScore());
    }


    static Stream<Arguments> fileTypeAndReaderProvider() {
        SpelGraphFactory spelGraphFactory = new SpelGraphFactory();
        UserService userService = new UserService();

        spelGraphFactory.registerHandler(DefaultNodeHandler.builder().handlerName("getUserSimpleInfoHandler")
                .action(userService::getUserSimpleInfoHandler).build());

        spelGraphFactory.registerHandler(DefaultNodeHandler.builder().handlerName("getUserCourseListHandler")
                .action(userService::getCourseList).build());

        spelGraphFactory.registerHandler(DefaultNodeHandler.builder().handlerName("getUserCourseScoreHandler")
                .action(userService::getCourseScoreList).build());

        return Stream.of(
                Arguments.of(DagGraphConfigType.JSON, spelGraphFactory),
                Arguments.of(DagGraphConfigType.YAML, spelGraphFactory)
        );
    }


    public UserInfoContext createUserContext(){
        UserInfoContext user = new UserInfoContext();
        user.setUid(123);
        return user;
    }


}