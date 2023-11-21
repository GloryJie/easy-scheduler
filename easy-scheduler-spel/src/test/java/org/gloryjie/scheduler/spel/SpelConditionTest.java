package org.gloryjie.scheduler.spel;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.core.MapDagContext;
import org.gloryjie.scheduler.reader.config.GraphDefinitionConfigReader;
import org.gloryjie.scheduler.reader.config.YamlGraphDefinitionReader;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class SpelConditionTest {



    GraphDefinitionConfigReader reader = new YamlGraphDefinitionReader();

    SpelGraphFactory spelGraphFactory = new SpelGraphFactory(reader);

    DagContext dagContext = createDagContext();


    @Test
    public void readContextConditionSuccessTest(){
        // read context info to evaluate
        String condition = "#{context.name == 'Jack' && context.age > 18 && context.uid == 123 && context.sex == 'female'}";
        Predicate<DagContext> predicate = spelGraphFactory.createCondition(condition);

        assertNotNull(predicate);
        assertTrue(predicate.test(dagContext));
    }

    @Test
    public void readContextConditionFailTest(){
        // read context info to evaluate
        String condition = "#{context.name == 'Jack' && context.age < 18 && context.uid == 123 && context.sex == 'female'}";
        Predicate<DagContext> predicate = spelGraphFactory.createCondition(condition);

        assertNotNull(predicate);
        assertFalse(predicate.test(dagContext));
    }


    @Test
    public void readValueConditionSuccessTest(){
        // read context info to evaluate
        String condition = "#{#course == 'java' && #score > 60}";
        Predicate<DagContext> predicate = spelGraphFactory.createCondition(condition);

        assertNotNull(predicate);
        assertTrue(predicate.test(dagContext));
    }


    @Test
    public void readValueConditionFailTest(){
        // read context info to evaluate
        String condition = "#{#course == 'java' && #score < 60}";
        Predicate<DagContext> predicate = spelGraphFactory.createCondition(condition);

        assertNotNull(predicate);
        assertFalse(predicate.test(dagContext));
    }


    @Test
    public void expressionErrTest(){
        // read context info to evaluate
        String condition = "#{context.noField == 'Jack'}";
        Predicate<DagContext> predicate = spelGraphFactory.createCondition(condition);

        assertNotNull(predicate);

        assertThrows(DagEngineException.class, ()->{
            predicate.test(dagContext);
        });

    }




    public DagContext createDagContext(){
        User user = new User();
        user.setName("Jack");
        user.setAge(22);
        user.setUid(123);
        user.setSex("female");

        MapDagContext mapDagContext = new MapDagContext(user);

        mapDagContext.put("course", "java");
        mapDagContext.put("score", 99);

        return mapDagContext;
    }



}