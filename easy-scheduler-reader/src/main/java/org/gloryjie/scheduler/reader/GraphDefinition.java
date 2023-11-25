package org.gloryjie.scheduler.reader;

import lombok.Getter;
import lombok.Setter;
import org.gloryjie.scheduler.reader.DagNodeDefinition;

import java.util.List;

@Getter
@Setter
public class GraphDefinition {

    private String graphName;
    private Long timeout;
    private String contextClass;
    private String initMethod;
    private String endMethod;
    private List<DagNodeDefinition> nodes;

}
