package com.thenetcircle.service.data.hive.udf.http;

import org.apache.http.concurrent.FutureCallback;

public interface IHttpClientCallback extends FutureCallback<Object[]> {

    @Override
    default void failed(final Exception ex) {
        // do something
    }

    @Override
    default void completed(final Object[] result) {
        // do something
        System.out.println(Thread.currentThread().getName() + result[0]);
    }

    @Override
    default void cancelled() {
        // do something
    }

}
