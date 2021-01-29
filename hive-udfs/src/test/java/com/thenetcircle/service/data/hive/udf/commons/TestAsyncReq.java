package com.thenetcircle.service.data.hive.udf.commons;

import com.thenetcircle.service.data.hive.udf.http.HttpHelper;
import com.thenetcircle.service.data.hive.udf.http.IHttpClientCallback;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

/**
 * @author: john
 * @created: 2021/01/29 16:07
 */
public class TestAsyncReq {

    private static Logger log = LoggerFactory.getLogger(TestAsyncReq.class);

    @Test
    public void testMultiReq(){
        


        Object[] ctx = new Object[] {
                "439963",
                "440713",
                "447882",
                "446633",
                "442012",
                "446334",
                "442636",
                "443284",
                "442855"
        };

        ThreadPoolExecutor threadPoolExecutor = HttpHelper.getInstance().getThreadPoolExecutor("a_http_get");
        log.info("\n--- start process --- \nwith ctx: {}\ncorePool size: {}, active: {}",
                ctx, threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getActiveCount());

        int start = 0;



    }



    public void executeFutureReq1(Object ctx, HttpRequestBase httpRequestBase) {
        log.info("submit url: {}, ctx: {}", httpRequestBase.getURI(), ctx);
        HttpHelper.getInstance().getFutureReqExecSvc().execute(
                httpRequestBase,
                HttpHelper.getHcContext(),
                new RespHandler(ctx),
                new IHttpClientCallback() {
                    @Override
                    public void completed(final Object[] result) {
                        synchronized (this) {
                            log.info(""+result);
                            log.info("\n\n -- process() --going to forward ctx: {} status: {}", result[3], result[0]);
                        }
                    }

                    @Override
                    public void failed(final Exception ex) {
                            synchronized (this) {
                                log.error("dsfds");
                            }
                    }

                    @Override
                    public void cancelled() {
                            synchronized (this) {
                                log.error("dsfds");
                            }
                    }
                });
    }


    final class RespHandler implements ResponseHandler<Object[]> {

        private Object ctx;

        public RespHandler(Object ctx) {
            this.ctx = ctx;
        }

        @Override
        public Object[] handleResponse(
                final HttpResponse response) throws ClientProtocolException, IOException {
            String resp = EntityUtils.toString(response.getEntity());
            log.info("threadInfo: {}, ctx: {}, handleResponse: {}",
                    NetUtil.getNet().getRunInfo(), ctx, resp.substring(0, 84));
            return new Object[]{
                    response.getStatusLine().getStatusCode(),
                    headers2Map(response.getAllHeaders()),
                    resp ,
                    ctx};
        }
    }

}
