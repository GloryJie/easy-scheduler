package org.gloryjie.scheduler.core;

public class DagEngineException extends RuntimeException {

    public DagEngineException(String message) {
        super(message);
    }


    public DagEngineException(String message, Throwable cause) {
        super(message, cause);
    }


}
