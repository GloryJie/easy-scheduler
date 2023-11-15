package org.gloryjie.scheduler.reader;

import java.util.List;
import java.util.Set;

public class GraphDefinition {

    private String graphName;
    private Long timeout;


    private List<DagNodeDefinition> nodes;


    public static class DagNodeDefinition {
        private String nodeName;

        private Long timeout;

        private Set<String> dependsOn;

        private String handlerName;

        private List<String> conditons;

        private List<String> actions;


    }


}
