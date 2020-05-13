package com.thenetcircle.service.data.hive.udf.commons;

public class MiscUtils {

    public static Long[] range(long start, long end, long step) {
        int len = (int) ((end - start) / step);
        Long[] arr = new Long[len];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = start + step * i;
        }
        return arr;
    }
}
