package org.gloryjie.scheduler.api;

import lombok.Getter;

@Getter
public enum DependencyType {

    STRONG(1),
    SOFT(2),
    WEAK(3);

    final int code;

    DependencyType(int code) {
        this.code = code;
    }


    public static DependencyType codeOf(int value) {
        for (DependencyType type : DependencyType.values()) {
            if (type.code == value) {
                return type;
            }
        }
        return null;
    }

}
