package org.gloryjie.scheduler.spel;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.core.MapDagContext;
import org.gloryjie.scheduler.reader.GraphDefinitionReader;
import org.gloryjie.scheduler.reader.YamlGraphDefinitionReader;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class SpelConsumerTest {



    GraphDefinitionReader reader = new YamlGraphDefinitionReader();

    SpelGraphFactory spelGraphFactory = new SpelGraphFactory(reader);

    DagContext dagContext = createDagContext();


    @Test
    public void readContextConditionSuccessTest(){
        // read context info to evaluate
        String condition = "#{context.name = 'Jack Ma'} #{context.age = 18} #{context.sex = 'male'} #{ put('B_value', 'B') }";
        Consumer<DagContext> predicate = spelGraphFactory.createConsumer(condition);

        assertNotNull(predicate);

        predicate.accept(dagContext);

        User user = (User) dagContext.getContext();
        assertNotNull(user);
        assertEquals("Jack Ma", user.getName());
        assertEquals(18, user.getAge());
        assertEquals("male", user.getSex());
    }



    @Test
    public void expressionErrTest(){
        // read context info to evaluate
        String condition = "#{context.noField == 'Jack'}";
        Consumer<DagContext> consumer = spelGraphFactory.createConsumer(condition);

        assertNotNull(consumer);

        assertThrows(DagEngineException.class, ()->{
            consumer.accept(dagContext);
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