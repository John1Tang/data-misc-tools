package com.thenetcircle.service.data.hive.udf.commons;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Snowflake {
    private static final long twepoch = 1288834974657L;
    private static final long workerIdBits = 10L;
    private static final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    private static final long sequenceBits = 12L;
    private static final long workerIdShift = sequenceBits;
    private static final long timestampLeftShift = sequenceBits + workerIdBits;
    private static final long sequenceMask = -1L ^ (-1L << sequenceBits);
    private static long workerId = 1;
    private static final AtomicLong sequence = new AtomicLong(0L);
    private static long lastTimestamp = -1L;

    public Snowflake() {
        try {
            this.workerId = Integer.valueOf(NetUtil.getNet().getHost());
        } catch (Exception e) {
            System.out.println("Exception in Snowflake constructor: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public Snowflake(long workerId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException("workerId can't be greater than %d or less than 0");
        }
        this.workerId = workerId;
    }

    public static synchronized long nextId() {
        long timestamp = getTime();

        if (timestamp < lastTimestamp) {
            throw new RuntimeException("Clock moved backwards");
        }

        if (lastTimestamp == timestamp) {
            sequence.set((sequence.get() + 1) & sequenceMask);
            if (sequence.get() == 0) {
                timestamp = tillNextMillis(lastTimestamp);
            }
        } else {
            sequence.set(0L);
        }

        lastTimestamp = timestamp;
        return ((timestamp - twepoch) << timestampLeftShift) | (workerId << workerIdShift) | sequence.get();
    }

    // we should increase workerId for each node.
    // this is left to the users to change accordingly
    private static long workerId() {
        return 1;
    }

    private static synchronized long tillNextMillis(long lastTimestamp) {
        long timestamp = getTime();
        while (timestamp <= lastTimestamp) {
            timestamp = getTime();
        }
        return timestamp;
    }

    private static synchronized long getTime() {
        return Instant.now().toEpochMilli();
    }

    public static void main(String[] args) {
        testNextId();
    }

    private static void testNextId() {
        int nodeCount = 10;

        // busy loop
//        ConcurrentLinkedQueue<Snowflake> workers = new ConcurrentLinkedQueue<Snowflake>();
        BlockingQueue<Snowflake> workers = new LinkedBlockingQueue<>();
        for (int i = 0; i < nodeCount; i++) {
            workers.add(new Snowflake(i));
        }

        ConcurrentLinkedQueue<Long> ids = new ConcurrentLinkedQueue<Long>();
        ExecutorService executor = Executors.newFixedThreadPool(100);

        for (int i = 0; i < 10000; i++) {
            // busy loop
//            executor.execute(() -> {
//                Snowflake worker = null;
//                while (worker == null) {
//                    worker = workers.poll();
//                }
//
//                ids.add(worker.nextId());
//                workers.add(worker);
//            });


            executor.execute(() -> {
                try {
                    Snowflake worker = workers.take();
                    ids.add(worker.nextId());
                    workers.add(worker);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

        }

        executor.shutdown(); // always remember to shutdown executor
        try {
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(ids.size());
        System.out.println(ids.stream().distinct().count());

        // pop and print first 100 elements from ConcurrentLinkedQueue
        for (int i = 0; i < 100; i++) {
            System.out.println(ids.poll());
        }

    }
}

