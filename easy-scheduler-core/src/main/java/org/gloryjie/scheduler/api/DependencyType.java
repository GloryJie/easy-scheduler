package org.gloryjie.scheduler.api;

import lombok.Getter;

@Getter
public enum DependencyType {

    STRONG(1),
    SOFT(2),
    WEAK(3);

    final int value;

    DependencyType(int value) {
        this.value = value;
    }

}
