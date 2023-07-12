package com.thenetcircle.service.data.hive.udf.commons;

import groovy.transform.ASTTest;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public String getHost(){
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            int lasted = NetUtil.getNet().getHost().lastIndexOf(".");
            return host.substring(lasted + 1);
        } catch (UnknownHostException e) {
            return "";
        }
    }


    public Long getRunTimeNo(){
        try {
            String hostName = InetAddress.getLocalHost().getHostAddress();
            int lasted = hostName.lastIndexOf(".");
            String host = String.format("%03d", Integer.parseInt(hostName.substring(lasted + 1)));

            String pidName = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            String pid = String.format("%07d", Integer.parseInt(pidName));

            String threadName = Thread.currentThread().getName();
            threadName = threadName.substring(threadName.lastIndexOf("-") + 1);
            threadName = "main".equals(threadName)? threadName = "0": threadName;
            String thread = String.format("%03d", Integer.parseInt(threadName));

            return Long.valueOf(host + pid + thread);
        } catch (Exception e) {
            return 0L;
        }
    }
    public String getSnakeRunInfoToProc(){
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

            return host + "_" + pid;
        } catch (UnknownHostException e) {
            return "";
        }
    }

    public static void main(String[] args) {

        System.out.println(NetUtil.getNet().getRunTimeNo());

        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 500; i++) {
            executor.submit(() -> {
                System.out.println(NetUtil.getNet().getRunTimeNo());
            });
        }

    }
}