package org.gloryjie.scheduler.reader;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.api.*;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.core.DefaultNodeHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractGraphFactoryTest {


    private AbstractGraphFactory graphFactory = new AbstractGraphFactory(){
        @Override
        public Predicate<DagContext> createCondition(String condition) {
            return null;
        }

        @Override
        public Consumer<DagContext> createConsumer(String action) {
            return null;
        }
    };

    private DagEngine dagEngine = new ConcurrentDagEngine();


    @Test
    void testClassGraphTest() throws Exception {
        Assertions.assertThrows(DagEngineException.class, ()->{
            DagGraph dagGraph = graphFactory.createClassGraph(UserInfoContext.class);
        });

        registerUserHandler(graphFactory);
        DagGraph dagGraph = graphFactory.createClassGraph(UserInfoContext.class);

        assertNotNull(dagGraph);
        assertEquals("org.gloryjie.scheduler.reader.UserInfoContext", dagGraph.getGraphName());
        assertNotNull(dagGraph.nodes());
        assertNotNull(dagGraph.getStartNode());
        assertNotNull(dagGraph.getEndNode());
        assertNotNull(dagGraph.getNodeInDegree());
        assertEquals(1, dagGraph.getNodeInDegree().get("userInfo"));
        assertEquals(1, dagGraph.getNodeInDegree().get("courseList"));
        assertEquals(1, dagGraph.getNodeInDegree().get("courseScoreList"));


        UserInfoContext userInfoContext = new UserInfoContext();
        userInfoContext.setUid(123);
        DagResult dagResult = dagEngine.fire(dagGraph, userInfoContext);

        // assert state
        assertTrue(dagResult.isDone());
        assertEquals(DagState.SUCCEED, dagResult.getState());
        assertTrue(dagResult.isDone());
        assertSame(DagState.SUCCEED, dagResult.getState());
        assertNull(dagResult.getThrowable());
        assertSame(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("userInfo"));
        assertSame(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("courseList"));
        assertSame(NodeState.SUCCEEDED, dagResult.getNodeStateMap().get("courseScoreList"));


        // assert result
        assertEquals("Jack", userInfoContext.getUserInfo().getName());
        assertEquals(22, userInfoContext.getUserInfo().getAge());
        assertEquals("Shenzhen", userInfoContext.getUserInfo().getAddress());

        assertEquals(Lists.newArrayList("Math", "Java", "Go", "Rust"), userInfoContext.getCourseList());

        assertEquals("Math", userInfoContext.getCourseScoreList().get(0).getCourseName());
        assertEquals(60, userInfoContext.getCourseScoreList().get(0).getScore());

        assertEquals("Java", userInfoContext.getCourseScoreList().get(1).getCourseName());
        assertEquals(70, userInfoContext.getCourseScoreList().get(1).getScore());
    }


    private void registerUserHandler(AbstractGraphFactory graphFactory){
        UserService userService = new UserService();

        graphFactory.registerHandler(DefaultNodeHandler.builder().handlerName("getUserSimpleInfoHandler")
                .action(userService::getUserSimpleInfoHandler).build());

        graphFactory.registerHandler(DefaultNodeHandler.builder().handlerName("getUserCourseListHandler")
                .action(userService::getCourseList).build());

        graphFactory.registerHandler(DefaultNodeHandler.builder().handlerName("getUserCourseScoreHandler")
                .action(userService::getCourseScoreList).build());

    }
}