package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.reader.annotation.GraphClassAnnotationGraphReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GraphClassAnnotationGraphReaderTest {


    GraphClassAnnotationGraphReader reader = new GraphClassAnnotationGraphReader();


    @Test
    public void readGraphClassTest() throws Exception {

        GraphDefinition graphDefinition = reader.read(UserInfoContext.class);

        assertEquals("org.gloryjie.scheduler.reader.UserInfoContext", graphDefinition.getGraphName());
        assertEquals(0, graphDefinition.getTimeout());
        assertEquals("init", graphDefinition.getInitMethod());
        assertEquals("end", graphDefinition.getEndMethod());
        assertEquals(3, graphDefinition.getNodes().size());

        DagNodeDefinition userInfoNode = graphDefinition.getNodes().get(0);
        assertEquals("userInfo", userInfoNode.getRetFieldName());
        assertEquals("getUserSimpleInfoHandler", userInfoNode.getHandler());


        DagNodeDefinition courseInfoNode = graphDefinition.getNodes().get(1);
        assertEquals("courseList", courseInfoNode.getRetFieldName());
        assertEquals("getUserCourseListHandler", courseInfoNode.getHandler());


    }


}