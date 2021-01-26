package com.thenetcircle.service.data.hive.udf.http;

import org.apache.http.concurrent.FutureCallback;

public interface IHttpClientCallback extends FutureCallback<ThreadLocal<Object[]>> {

    @Override
    default void failed(final Exception ex) {
        // do something
    }

    @Override
    default void completed(final ThreadLocal<Object[]> result) {
        // do something
        System.out.println(Thread.currentThread().getName() + result.get()[0]);
    }

    @Override
    default void cancelled() {
        // do something
    }

}
