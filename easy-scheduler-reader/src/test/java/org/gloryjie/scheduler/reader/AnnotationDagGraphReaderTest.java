package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.reader.annotation.AnnotationDagGraphReader;
import org.gloryjie.scheduler.reader.data.UserInfoContext;
import org.gloryjie.scheduler.reader.definition.DagNodeDefinition;
import org.gloryjie.scheduler.reader.definition.GraphDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AnnotationDagGraphReaderTest {


    AnnotationDagGraphReader reader = new AnnotationDagGraphReader();


    @Test
    public void readGraphClassTest() throws Exception {

        GraphDefinition graphDefinition = reader.read(UserInfoContext.class);

        assertEquals("org.gloryjie.scheduler.reader.data.UserInfoContext", graphDefinition.getGraphName());
        assertEquals(0, graphDefinition.getTimeout());

        assertEquals(2, graphDefinition.getNodes().size());

        DagNodeDefinition userInfoNode = graphDefinition.getNodes().get(0);
        assertEquals("userInfo", userInfoNode.getRetFieldName());
        assertEquals("getUserSimpleInfoHandler", userInfoNode.getHandler());


        DagNodeDefinition courseInfoNode = graphDefinition.getNodes().get(1);
        assertEquals("courses", courseInfoNode.getRetFieldName());
        assertEquals("getUserCoursesHandler", userInfoNode.getHandler());


    }


}