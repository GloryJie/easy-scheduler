package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.api.NodeHandler;

import javax.annotation.Nullable;

public interface HandlerRegistry {


    @Nullable
    NodeHandler getHandler(String handlerName);


    void registerHandler(NodeHandler handler);


}
