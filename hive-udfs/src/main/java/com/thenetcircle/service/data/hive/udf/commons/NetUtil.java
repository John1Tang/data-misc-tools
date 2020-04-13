package com.thenetcircle.service.data.hive.udf.commons;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetUtil {

    private NetUtil() {
    }

    private static class NetUtilHolder{
        private static final NetUtil INSTANCE = new NetUtil();
    }

    public static NetUtil getNet() {
        return NetUtilHolder.INSTANCE;
    }

    public String getRunInfo(){
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            String threadName = Thread.currentThread().getName();

            return host + ";;;" + pid + ";;;" + threadName;
        } catch (UnknownHostException e) {
            return "";
        }
    }
}