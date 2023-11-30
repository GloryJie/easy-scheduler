package org.gloryjie.scheduler.example.user;

import org.gloryjie.scheduler.api.DagGraph;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.dynamic.DefaultDynamicDagEngine;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.reader.config.DagGraphConfigType;
import org.gloryjie.scheduler.spel.SpelGraphFactory;
import org.springframework.util.StreamUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConfigUserLauncher {

    public static void main(String[] args) throws Exception {
        SpelGraphFactory spelGraphFactory = new SpelGraphFactory();
        ConcurrentDagEngine concurrentDagEngine = new ConcurrentDagEngine();
        DynamicDagEngine dagEngine = new DefaultDynamicDagEngine(spelGraphFactory, concurrentDagEngine);

        UserService userService = new UserService();
        // Auto convert the UserService's method as handler and register in the dagEngine
        dagEngine.registerMethodHandler(userService);

        // load config
        InputStream inputStream = ConfigUserLauncher.class.getClassLoader().getResourceAsStream("userGraph.yml");
        String config = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);

        // user graph factory to create graph
        List<DagGraph> graph = spelGraphFactory.createConfigGraph(DagGraphConfigType.YAML, config);
        DagGraph dagGraph = graph.get(0);

        UserContext userContext = new UserContext();
        userContext.setUid(123);

        DagResult dagResult = dagEngine.fire(dagGraph, userContext);

        System.out.println(dagResult);
    }

}
