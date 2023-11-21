package org.gloryjie.scheduler.reader.definition;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class GraphDefinition {

    private String graphName;
    private Long timeout;
    private String contextClass;
    private List<DagNodeDefinition> nodes;

}
