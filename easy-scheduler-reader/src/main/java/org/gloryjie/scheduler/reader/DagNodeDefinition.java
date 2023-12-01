package org.gloryjie.scheduler.reader;

import lombok.Getter;
import lombok.Setter;
import org.gloryjie.scheduler.api.DependencyType;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class DagNodeDefinition {

    private String nodeName;

    private String retFieldName;

    private Long timeout;

    private Set<String> dependsOn;

    Map<DependencyType, Set<String>> dependsOnType;

    private String handler;

    private List<String> conditions;

    private List<String> actions;

    private String paramConverter;

    private String retConverter;

}
