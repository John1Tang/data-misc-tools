package com.thenetcircle.service.data.hive.udf.commons;

import com.google.common.collect.Lists;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.FutureRequestExecutionService;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpRequestFutureTask;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestAsyncHttpClient {

    private Logger log = LoggerFactory.getLogger(TestAsyncHttpClient.class);

    @Test
    public void testAsyncHttp() throws ExecutionException, InterruptedException {

        HttpClient httpClient = HttpClientBuilder.create().setMaxConnPerRoute(5).build();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        FutureRequestExecutionService futureRequestExecutionService =
                new FutureRequestExecutionService(httpClient, executorService);

        List<String> urls = Arrays.asList("http://www.bing.com", "http://www.baidu.com", "http://www.qq.com");
        List<HttpRequestFutureTask<ResponseResult>> reqTasks = Lists.newArrayListWithCapacity(3);
        for (String url : urls) {
            HttpRequestFutureTask<ResponseResult> task = futureRequestExecutionService.execute(
                    new HttpGet(url), HttpClientContext.create(),
                    new OkidokiHandler(), new MyCallback());
            reqTasks.add(task);
        }
        Thread.sleep(8000);

    }

    private final class OkidokiHandler implements ResponseHandler<ResponseResult> {
        public ResponseResult handleResponse(
                final HttpResponse response) throws ClientProtocolException, IOException {
            return new ResponseResult(response.getStatusLine().getStatusCode(),
                    EntityUtils.toString(response.getEntity()));
        }
    }

    class ResponseResult{
        private int code;
        String content;

        public ResponseResult(int code, String content) {
            this.code = code;
            this.content = content;
        }

    }

    private final class MyCallback implements FutureCallback<ResponseResult> {

        public void failed(final Exception ex) {
            // do something
        }

        public void completed(final ResponseResult result) {
            // do something
            System.out.println(Thread.currentThread().getName() + result.code);
        }

        public void cancelled() {
            // do something
        }
    }
}
