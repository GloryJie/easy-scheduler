package org.gloryjie.scheduler.annotation;

import java.lang.annotation.*;


@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface NodeHandler {

    String name();

    String condition();

}
