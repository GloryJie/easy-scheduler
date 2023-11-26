package org.gloryjie.scheduler.core;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.ExecutorSelector;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SingleExcutorSelector implements ExecutorSelector {

    private final ExecutorService executorService;

    private final AtomicInteger threadCount = new AtomicInteger(0);

    public SingleExcutorSelector(int core) {
        executorService = new ThreadPoolExecutor(core, core, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName("easy-scheduler-" + threadCount.getAndIncrement());
                    thread.setUncaughtExceptionHandler((t, e) -> {
                        log.error("easy-scheduler thread error", e);
                    });
                    return thread;
                });
    }


    @Override
    public ExecutorService select(String graphName) {
        return executorService;
    }
}
