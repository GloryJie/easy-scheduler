package org.gloryjie.scheduler.reader.annotation;

import java.lang.annotation.*;

@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ContextParam {

    /**
     * The name of the context parameter to bind to.
     * first get from user context, second from dag context
     */
    String value() default "";

    boolean required() default true;

}
