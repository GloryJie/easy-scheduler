package org.gloryjie.scheduler.dynamic;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.dynamic.annotation.ContextParam;
import org.gloryjie.scheduler.dynamic.annotation.MethodNodeHandler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class MethodNodeHandlerImpl implements NodeHandler<Object> {


    private final Object bean;

    private final Method method;

    private final MethodNodeHandler annotation;


    public MethodNodeHandlerImpl(Object bean, Method method, MethodNodeHandler annotation) {
        this.bean = bean;
        this.method = method;
        this.annotation = annotation;
    }


    @Override
    public String handlerName() {
        return annotation.value();
    }


    @Override
    public Object execute(DagNode dagNode, DagContext dagContext) {

        Object[] methodArg = null;

        try {
            methodArg = readMethodArg(dagContext);
        } catch (Exception e) {
            String msg = String.format("NodeHandler[%s] failed to read method[%s] args", handlerName(), method.getName());
            throw new DagEngineException(msg, e);
        }

        try {
            return MethodUtils.invokeMethod(bean, method.getName(), methodArg);
        } catch (Exception e) {
            String msg = String.format("Failed to invoke method: %s", method.getName());
            throw new DagEngineException(msg, e);
        }
    }

    @Override
    public Long timeout() {
        return annotation.timeout();
    }

    private Object[] readMethodArg(DagContext dagContext) throws Exception {
        Parameter[] parameters = method.getParameters();
        if (parameters == null || parameters.length == 0) {
            return ArrayUtils.EMPTY_OBJECT_ARRAY;
        }

        Object[] args = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            ContextParam contextParam = parameter.getAnnotation(ContextParam.class);
            Object arg = null;

            if (contextParam != null) {
                String paramName = contextParam.value();

                arg = readValueByName(paramName, dagContext);

                if (arg == null && contextParam.required()) {
                    throw new DagEngineException(String.format("could not get param[%s] from context", paramName));
                }
            } else {
                arg = readValueByName(parameter.getName(), dagContext);
            }

            args[i] = arg;
        }

        return args;
    }


    private Object readValueByName(String name, DagContext dagContext) throws Exception {
        Object value = null;

        Object userContext = dagContext.getContext();

        // read from user context first
        if (userContext != null) {
            Field field = FieldUtils.getField(userContext.getClass(), name, true);
            if (field != null) {
                value = FieldUtils.readField(field, userContext, true);
            }
        }

        // read from dag context
        if (value == null) {
            value = dagContext.get(name);
        }

        return value;
    }


}
