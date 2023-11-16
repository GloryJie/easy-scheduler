package org.gloryjie.scheduler.reader;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class GraphDefinition {

    private String graphName;
    private Long timeout;
    private List<DagNodeDefinition> nodes;

}
