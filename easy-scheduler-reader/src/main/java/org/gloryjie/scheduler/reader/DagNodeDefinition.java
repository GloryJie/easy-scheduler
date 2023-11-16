package org.gloryjie.scheduler.reader;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Set;

@Getter
@Setter
public class DagNodeDefinition {

    private String nodeName;

    private Long timeout;

    private Set<String> dependsOn;

    private String handler;

    private List<String> conditions;

    private List<String> actions;

}
