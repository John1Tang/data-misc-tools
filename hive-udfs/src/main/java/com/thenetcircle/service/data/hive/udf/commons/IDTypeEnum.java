package com.thenetcircle.service.data.hive.udf.commons;

import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;

public enum IDTypeEnum {
    ACTOR_ID(1),
    DEFAULT(0);

    private final int code;

    IDTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static final Map<Integer, IDTypeEnum> LOOKUP = Maps.uniqueIndex(
            Arrays.asList(IDTypeEnum.values()),
            IDTypeEnum::getCode
    );
}
