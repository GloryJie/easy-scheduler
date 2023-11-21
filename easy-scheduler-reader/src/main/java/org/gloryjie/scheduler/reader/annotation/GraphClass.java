package org.gloryjie.scheduler.reader.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GraphClass {

    String graphName() default "";

    long timeout() default 0;

    String initMethod() default "";
}
