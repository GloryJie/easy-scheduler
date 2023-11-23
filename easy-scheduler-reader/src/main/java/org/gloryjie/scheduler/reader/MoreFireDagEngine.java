package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.DagEngine;
import org.gloryjie.scheduler.api.DagResult;

public interface MoreFireDagEngine extends DagEngine {


    DagResult fireWithContext(Object context);

    DagResult fireWithConfig(String graphConfig);

    DagResult fireWithGraphName(String graphName);

}
