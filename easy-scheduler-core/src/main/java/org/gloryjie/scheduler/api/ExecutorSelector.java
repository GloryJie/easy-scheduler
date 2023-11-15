package org.gloryjie.scheduler.api;

import java.util.concurrent.ExecutorService;

public interface ExecutorSelector {

    ExecutorService select(String graphName);

}
