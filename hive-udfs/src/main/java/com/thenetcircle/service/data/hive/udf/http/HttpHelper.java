package com.thenetcircle.service.data.hive.udf.http;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.thenetcircle.service.data.hive.udf.commons.NamedThreadFactory;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.FutureRequestExecutionService;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgPrimitive;
import static com.thenetcircle.service.data.hive.udf.UDFHelper.getConstantIntValue;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

/**
 * @author john
 */
public class HttpHelper {

    static HttpClientContext hcContext;
    volatile RequestConfig rc;
    transient volatile ThreadPoolExecutor threadPoolExecutor;
    transient volatile FutureRequestExecutionService futureRequestExecutionService;
    transient volatile CloseableHttpClient hc;

    transient StringObjectInspector urlInsp;
    transient MapObjectInspector headersInsp;
    transient StringObjectInspector contentInsp;


    private int timeout;
    private int coreSize;
    private transient final int coreNum;

    private HttpHelper(){
        hcContext = HttpClientContext.create();
        coreNum = Runtime.getRuntime().availableProcessors();
        coreSize = coreNum;
    }

    private static final Supplier<HttpHelper> instance = Suppliers.memoize(HttpHelper::new);

    public static HttpHelper getInstance() {
        return instance.get();
    }

    public static HttpClientContext getHcContext() {
        return hcContext;
    }

    public static Map<String, String> headers2Map(Header... headers) {
        Map<String, String> re = new HashMap<>();
        if (ArrayUtils.isEmpty(headers)) {
            return re;
        }
        for (Header header : headers) {
            re.put(header.getName(), header.getValue());
        }
        return re;
    }

    public static Header[] map2Headers(Map<?, ?> map) {
        if (MapUtils.isEmpty(map)) {
            return null;
        }
        return map.entrySet().stream()
            .map(en -> new BasicHeader(String.valueOf(en.getKey()), String.valueOf(en.getValue())))
            .toArray(Header[]::new);
    }

    public static final List<String> RESULT_FIELDS = Arrays.asList("code", "headers", "content");
    public static final List<ObjectInspector> RESULT_FIELD_INSPECTORS = Arrays.asList(
        javaIntObjectInspector,
        getStandardMapObjectInspector(
            javaStringObjectInspector,
            javaStringObjectInspector),
        javaStringObjectInspector);

    public static final StandardStructObjectInspector RESULT_TYPE = ObjectInspectorFactory.getStandardStructObjectInspector(
        RESULT_FIELDS,
        RESULT_FIELD_INSPECTORS);


    public static final List<String> ASYNC_RESULT_FIELDS = Arrays.asList("code", "headers", "content");
    public static final List<ObjectInspector> ASYNC_RESULT_FIELD_INSPECTORS = Arrays.asList(
            javaIntObjectInspector,
            getStandardMapObjectInspector(
                    javaStringObjectInspector,
                    javaStringObjectInspector),
            javaStringObjectInspector);

    public static final StandardStructObjectInspector ASYNC_RESULT_TYPE = ObjectInspectorFactory.getStandardStructObjectInspector(
            ASYNC_RESULT_FIELDS,
            ASYNC_RESULT_FIELD_INSPECTORS);



    public static Object[] runtimeErr(String errMsg) {
        return new Object[]{-1, null, errMsg};
    }

    public static Object[] runtimeErr(Object ctx, String errMsg) {
        return new Object[]{-1, null, errMsg,ctx};
    }

    public static Object[] runtimeErr(Throwable e) {
        return new Object[]{-1, null, e.toString()};
    }


    public Object[] sendAndGetHiveResult(HttpUriRequest req) {
        try {
            HttpResponse resp = getHttpClient().execute(req);
            return new Object[]{
                resp.getStatusLine().getStatusCode(),
                headers2Map(resp.getAllHeaders()),
                EntityUtils.toString(resp.getEntity())};
        } catch (IOException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            return runtimeErr(e);
        }
    }

    public static final String UTF_8 = "UTF-8";

    public static HttpGet reqGet(String urlStr, Map<String, String> headersMap) {
        HttpGet get = new HttpGet(urlStr);
        get.setHeaders(map2Headers(headersMap));
        return get;
    }

    public static HttpPost reqPost(String urlStr, Map<String, String> headersMap, String content) {
        HttpPost post = new HttpPost(urlStr);
        post.setHeaders(map2Headers(headersMap));
        if (isEmpty(content)) {
            return post;
        }
        try {
            post.setEntity(new StringEntity(content));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
        }
        return post;
    }

    public CloseableHttpClient getHttpClient() {
        if (null == hc) {
            synchronized (this) {
                if (null == hc) {
                    hc = HttpClientBuilder
                            .create()
                            .setKeepAliveStrategy((response, context) -> {
                                HeaderElementIterator it = new BasicHeaderElementIterator
                                        (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                                while (it.hasNext()) {
                                    HeaderElement he = it.nextElement();
                                    String param = he.getName();
                                    String value = he.getValue();
                                    if (value != null && param.equalsIgnoreCase
                                            ("timeout")) {
                                        return Long.parseLong(value) * 1000;
                                    }
                                }
                                return 60 * 1000;
                            })
                            // default set to retry 3 times
                            .setRetryHandler(new DefaultHttpRequestRetryHandler(3, true))
                            .build();
                }
            }
        }
        return hc;
    }

    public ThreadPoolExecutor getThreadPoolExecutor(String name) {
        if (null == threadPoolExecutor) {
            synchronized (this) {
                if (null == threadPoolExecutor) {
                    threadPoolExecutor = new ThreadPoolExecutor(coreSize * 2, coreSize * 20, 8, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>(1000), new NamedThreadFactory(name), new ThreadPoolExecutor.CallerRunsPolicy());
                }
            }
        }
        return threadPoolExecutor;
    }

    public FutureRequestExecutionService getFutureReqExecSvc() {
        if (null == futureRequestExecutionService){
            synchronized (this) {
                if (null == futureRequestExecutionService) {
                    futureRequestExecutionService = new FutureRequestExecutionService(HttpHelper.getInstance().getHttpClient(), threadPoolExecutor);
                }
            }
        }
        return futureRequestExecutionService;
    }

    /*public void executeFutureReq(Object ctx, HttpRequestBase httpRequestBase, ConcurrentLinkedQueue<Object[]> resultQueue) {
        HttpHelper.getInstance().getFutureReqExecSvc().execute(
                httpRequestBase,
                hcContext,
                new RespHandler(ctx),
                new IHttpClientCallback() {
                    @Override
                    public void completed(Object[] result) {
                        resultQueue.offer(result);
                    }

                    @Override
                    public void failed(Exception ex) {
                        resultQueue.offer(runtimeErr(ctx, ex.getMessage()));
                    }

                    @Override
                    public void cancelled() {
                        resultQueue.offer(runtimeErr(ctx, "task cancelled"));
                    }
                });
    }*/

    public void closeHttpClient() throws IOException {
        hc = HttpHelper.getInstance().getHttpClient();
        if (hc == null) {
            return;
        }
        try {
            hc.close();
            hc = null;
        } catch (IOException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            throw new IOException(e);
        }
    }

    public void closeFutureReqExecSvc() throws HiveException {
        try {
            futureRequestExecutionService.close();
            // in case once in second execution found out closed state is true
            // and report illegal state
            futureRequestExecutionService = null;
        } catch (IOException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            throw new HiveException(e);
        }
    }

    public void setUrl(ObjectInspector[] args, int idx){
        this.urlInsp = (StringObjectInspector) args[idx];
    }

    public void setCoreSize(ObjectInspector[] args, int idx, String udfName) throws UDFArgumentTypeException {
        if (args.length - 1 < idx) {
            return;
        }
        checkArgPrimitive(udfName, args, idx);
        coreSize = Optional.ofNullable(getConstantIntValue(udfName, args, idx)).orElse(coreNum);
        if (coreSize < 1 || coreSize > coreNum) {
            coreSize = coreNum;
        }

    }


    public void setContent(ObjectInspector[] args, int idx, String udfName) throws UDFArgumentTypeException {
        if (args.length - 1 < idx) {
            return;
        }
        checkArgPrimitive(udfName, args, idx);
        ObjectInspector contentObj = args[idx];
        if (!(args[idx] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(idx, "content must be string");
        }
        this.contentInsp = (StringObjectInspector) contentObj;
    }

    public void setTimeout(ObjectInspector[] args, int idx, String udfName) throws UDFArgumentTypeException {
        if (args.length - 1 < idx) {
            return;
        }
        checkArgPrimitive(udfName, args, idx);
        timeout = Optional.ofNullable(getConstantIntValue(udfName, args, idx)).orElse(0);
        rc = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
    }

    public void setReqHeader(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length - 1 < idx) {
            return;
        }
        ObjectInspector headerInsp = args[idx];
        if (headerInsp instanceof WritableVoidObjectInspector) {
            headersInsp = null;
        } else {
            if (!(headerInsp instanceof MapObjectInspector)) {
                throw new UDFArgumentTypeException(idx, "header parameter must be map<string, object> or null:\n\t" + args[idx]);
            }
            MapObjectInspector moi = (MapObjectInspector) headerInsp;
            if (!(moi.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(idx, "header parameter must be map<string, object>");
            }
            headersInsp = moi;
        }
    }

    /**
     * generate a prototype HttpPost
     *
     * @param args
     * @param idxHeader
     * @param idxContent
     * @return
     * @throws HiveException
     */
    HttpRequestBase setHttpPost(Object[] args, int idxUrl, int idxHeader, int idxContent) throws HiveException {
        String url = getUrl(args[idxUrl]);
        return setHttpReqBase(args, idxHeader, idxContent, new HttpPost(url));
    }

    HttpRequestBase setHttpDelete(Object[] args, int idxUrl, int idxHeader, int idxContent) throws HiveException {
        String url = getUrl(args[idxUrl]);
        return setHttpReqBase(args, idxHeader, idxContent, new HttpDelete(url));
    }

    HttpRequestBase setHttpGet(Object[] args, int idxUrl, int idxHeader) throws HiveException {
        String url = getUrl(args[idxUrl]);
        return setHttpReqBase(args, idxHeader, 0, new HttpGet(url));
    }


    private HttpRequestBase setHttpReqBase(Object[] args, int idxHeader, int idxContent, HttpRequestBase httpRequestBase) throws HiveException {

        httpRequestBase.setConfig(rc);
        Header[] headers;

        if (args.length > idxHeader && args[idxHeader] != null && headersInsp != null) {
            Map<?, ?> headersMap = headersInsp.getMap(args[idxHeader]);
            headers = map2Headers(headersMap);
            httpRequestBase.setHeaders(headers);
        }

        List<String> methodList = Arrays.asList("get", "delete");
        String methodName = httpRequestBase.getMethod();
        if (methodList.stream().noneMatch(e -> e.equalsIgnoreCase(methodName))) {
            if (args.length > idxContent) {
                String content = contentInsp.getPrimitiveJavaObject(args[idxContent]);
                try {
                    ((HttpEntityEnclosingRequestBase)httpRequestBase).setEntity(new StringEntity(content));
                } catch (UnsupportedEncodingException e) {
                    throw new HiveException("url is blank");
                }
            }
        }

        return httpRequestBase;
    }

    private String getUrl(Object arg) throws HiveException {
        String urlStr = this.urlInsp.getPrimitiveJavaObject(arg);
        if (StringUtils.isBlank(urlStr)) {
            throw new HiveException("url is blank");
        }
        return urlStr;
    }
}
