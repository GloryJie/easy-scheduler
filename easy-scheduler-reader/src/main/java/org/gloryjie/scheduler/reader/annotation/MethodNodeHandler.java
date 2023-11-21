package org.gloryjie.scheduler.reader.annotation;


import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MethodNodeHandler {

    String value();

    long timeout() default 0;

    String[] conditions() default {};


}
