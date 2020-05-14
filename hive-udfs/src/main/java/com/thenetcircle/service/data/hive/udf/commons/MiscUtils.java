package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.http.util.Asserts;

public class MiscUtils {

    public static Long[] range(long start, long end, long step) {
        Asserts.check(step > 0 && start >= 0 && end > 0, "each param should be greater than 0");
        Asserts.check(start <= end, "end should be greater than start");
        int len = (int) ((end - start) % step == 0 ? (end - start) / step : (end - start) / step + 1);
        Long[] arr = new Long[len];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = start + step * i;
        }
        return arr;
    }
}
