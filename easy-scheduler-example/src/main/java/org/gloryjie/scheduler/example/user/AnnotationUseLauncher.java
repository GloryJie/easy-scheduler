package org.gloryjie.scheduler.example.user;

import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.api.DagState;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.dynamic.DefaultDynamicDagEngine;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.spel.SpelGraphFactory;

public class AnnotationUseLauncher {

    public static void main(String[] args) {

        SpelGraphFactory spelGraphFactory = new SpelGraphFactory();
        ConcurrentDagEngine concurrentDagEngine = new ConcurrentDagEngine();
        DynamicDagEngine dagEngine = new DefaultDynamicDagEngine(spelGraphFactory);

        UserService userService = new UserService();
        // Auto convert the UserService's method as handler and register in the dagEngine
        dagEngine.registerMethodHandler(userService);

        UserContext userContext = new UserContext();
        userContext.setUid(123);
        DagResult dagResult = dagEngine.fireContext(userContext);
        if (dagResult.getState() == DagState.SUCCEED) {
            System.out.println(userContext.toString());
        } else {
            dagResult.getThrowable().printStackTrace();
        }
    }

}
