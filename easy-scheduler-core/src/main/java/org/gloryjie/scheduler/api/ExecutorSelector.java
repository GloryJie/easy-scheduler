package org.gloryjie.scheduler.api;

import java.util.concurrent.ExecutorService;

public interface ExecutorSelector {

    /**
     * Selects an executor service based on the given graph name.
     *
     * @param graphName The name of the graph.
     * @return The selected executor service.
     */
    ExecutorService select(String graphName);

}
