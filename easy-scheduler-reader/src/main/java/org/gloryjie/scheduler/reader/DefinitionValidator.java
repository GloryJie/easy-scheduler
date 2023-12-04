package org.gloryjie.scheduler.reader;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class DefinitionValidator {

    public void checkNodeDefinition(DagNodeDefinition definition) {
        if (StringUtils.isEmpty(definition.getNodeName())) {
            throw new DagGraphReadException("DagNode name cannot be empty");
        }

        if (StringUtils.isEmpty(definition.getHandler())
                && CollectionUtils.isEmpty(definition.getActions())) {
            throw new DagGraphReadException("DagNode handler or actions cannot be empty");
        }
    }

    public void checkGraphDefinition(GraphDefinition definition) {
        if (StringUtils.isEmpty(definition.getGraphName())) {
            throw new DagGraphReadException("Graph name cannot be empty");
        }

        if (CollectionUtils.isEmpty(definition.getNodes())) {
            throw new DagGraphReadException("Graph nodes cannot be empty");
        }

        for (DagNodeDefinition nodeDefinition : definition.getNodes()) {
            checkNodeDefinition(nodeDefinition);
        }

    }


}
