package reader;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.api.*;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.dynamic.DefaultDynamicDagEngine;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.reader.AbstractGraphFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reader.data.UserInfoContext;
import reader.data.UserService;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultDynamicDagEngineTest {


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

    private DynamicDagEngine dynamicDagEngine = new DefaultDynamicDagEngine(graphFactory, new ConcurrentDagEngine());


    @Test
    void testClassGraphTest() throws Exception {
        Assertions.assertThrows(DagEngineException.class, ()->{
            DagGraph dagGraph = graphFactory.createClassGraph(UserInfoContext.class);
        });

        dynamicDagEngine.registerMethodHandler(new UserService());
        dynamicDagEngine.registerGraphClass(UserInfoContext.class);


        UserInfoContext userInfoContext = new UserInfoContext();
        userInfoContext.setUid(123);
        DagResult dagResult = dynamicDagEngine.fireContext(userInfoContext);

        // assert state
        assertTrue(dagResult.isDone());
        assertEquals(DagState.SUCCESS, dagResult.getState());
        assertTrue(dagResult.isDone());
        assertSame(DagState.SUCCESS, dagResult.getState());
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
}