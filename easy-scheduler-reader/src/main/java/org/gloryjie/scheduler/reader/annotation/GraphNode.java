package org.gloryjie.scheduler.reader.annotation;


import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface GraphNode {

    String name() default "";

    long timeout() default 0;

    String handler() default "";

    String[] conditions() default {};

    String[] actions() default {};

    String[] dependsOn() default {};

    String paramConverter() default "";

    String retConverter() default "";


}
