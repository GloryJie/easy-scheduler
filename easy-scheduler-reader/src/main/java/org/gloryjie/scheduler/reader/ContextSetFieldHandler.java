package org.gloryjie.scheduler.reader;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.DagEngineException;

import java.lang.reflect.Field;

public class ContextSetFieldHandler implements NodeHandler<Object> {

    private final NodeHandler<Object> handler;

    private final Field field;

    public ContextSetFieldHandler(NodeHandler<Object> handler, Field field) {
        this.handler = handler;
        this.field = field;
    }

    @Override
    public String handlerName() {
        return handler.handlerName();
    }

    @Override
    public Object execute(DagContext dagContext) {
        Object result = handler.execute(dagContext);
        Object context = dagContext.getContext();
        try {
            if (result != null && context != null) {
                FieldUtils.writeField(field, context, result, true);
            }
        } catch (Exception e) {
            String msg = String.format("Failed to set field: %s to context: %s", field.getName(),
                    context.getClass().getSimpleName());
            throw new DagEngineException(msg, e);
        }
        return result;
    }

    @Override
    public Long timeout() {
        return handler.timeout();
    }

    @Override
    public boolean evaluate(DagContext dagContext) {
        return handler.evaluate(dagContext);
    }
}
