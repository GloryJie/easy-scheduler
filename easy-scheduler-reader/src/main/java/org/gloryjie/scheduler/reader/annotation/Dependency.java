package org.gloryjie.scheduler.reader.annotation;

import org.gloryjie.scheduler.api.DependencyType;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface Dependency {

    DependencyType type() default DependencyType.STRONG;

    String[] on() default {};


}
