package com.thenetcircle.service.data.hive.udf.commons;

import com.thenetcircle.service.data.hive.udf.http.HttpHelper;
import com.thenetcircle.service.data.hive.udf.http.IHttpClientCallback;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgPrimitive;
import static com.thenetcircle.service.data.hive.udf.UDFHelper.getConstantIntValue;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

/**
 * @author: john
 * @created: 2021/01/29 16:07
 */
public class TestAsyncReq {

    private static Logger log = LoggerFactory.getLogger(TestAsyncReq.class);


    private transient LongAccumulator processCounter = new LongAccumulator((x, y) -> x + y, 0);
    private transient LongAdder forwardCounter = new LongAdder();

    volatile RequestConfig rc;
    private static int TIMEOUT = 30_000;
    private static String urlTemplate = "https://helpdeskkaufmich.kayako.com/api/v1/cases/%s/posts.json?include=&limit=20";

    private static Map<String, String> initHeader () {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Authorization", "Basic cG9wcGVuZGV2QHRoZW5ldGNpcmNsZS5jb206S2F5YWtvdGVzdEAyMDE3");
        headerMap.put("X-API-Token", "440be73f-7a9a-492f-a88b-745e5affb049");
        headerMap.put("Content-Type", "application/json; charset=UTF-8");
        headerMap.put("Accept", "application/json");
        headerMap.put("Accept-Encoding", "gzip, compress, br");
        headerMap.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36");
        headerMap.put("X-Session-ID", "PKY6Z6xCmVooYx1jUXlQx0I0103724f6dad1ff0c80fbd33d56222a307a083c3WCKbwV9WyN8UyZ554oz1");
        headerMap.put("X-CSRF-Token", "ZCeSnUFdHGDKMosJtSi8Yn24lOQzgcccX9B1ubjzZHVQVgLuZ1wxnWX2UXey5yK6mpnIWl6PVq99Lj2QQAeKN8URwzrxEPvtXDTX");
        return headerMap;
    }

    private RequestConfig getRc() {
        return RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT).setConnectionRequestTimeout(TIMEOUT).build();
    }

    public static Header[] map2Headers(Map<?, ?> map) {
        if (MapUtils.isEmpty(map)) {
            return null;
        }
        return map.entrySet().stream()
                .map(en -> new BasicHeader(String.valueOf(en.getKey()), String.valueOf(en.getValue())))
                .toArray(Header[]::new);
    }


    @Test
    public void testMultiReq() {

        HttpHelper hh = HttpHelper.getInstance();

        Object[] ctxs = new Object[] {
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

        // init ThreadPool
        ThreadPoolExecutor threadPoolExecutor = hh.getThreadPoolExecutor("a_http_get");

        Header[] headers = map2Headers(initHeader());

        for (Object ctx : ctxs) {
            processCounter.accumulate(1);
            String url = String.format(urlTemplate, ctx);
            HttpGet httpGet = new HttpGet(url);
            httpGet.setHeaders(headers);
            httpGet.setConfig(getRc());
            executeFutureReq1(hh, ctx, httpGet);
        }

        while (processCounter.longValue() > forwardCounter.longValue()) {
            MiscUtils.easySleep(1000);
            System.out.println("\n\n -- close() -- waited 1 second but not result in queue yet! forward size: " + forwardCounter.longValue() + "\n\n");
        }

    }




    public void executeFutureReq1(HttpHelper hh, Object ctx, HttpRequestBase httpRequestBase) {
        System.out.println("submit url: " + httpRequestBase.getURI() + ", ctx: " + ctx);
        hh.getFutureReqExecSvc().execute(
                httpRequestBase,
                HttpHelper.getHcContext(),
                new RespHandler(ctx),
                new IHttpClientCallback() {
                    @Override
                    public void completed(final Object[] result) {
                        synchronized (this) {
                            System.out.println(""+result);
                            System.out.println("\n\n -- process() --going to forward ctx: "+result[3]+" status: "+result[0]);
                            forwardCounter.increment();
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
            System.out.println("threadInfo: " + NetUtil.getNet().getRunInfo()
                    +  ", ctx: " + ctx
                    + ", handleResponse: " + resp.substring(0, 84));
            return new Object[]{
                    response.getStatusLine().getStatusCode(),
                    headers2Map(response.getAllHeaders()),
                    resp ,
                    ctx};
        }
    }

}
