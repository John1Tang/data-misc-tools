package com.thenetcircle.service.data.hive.udf.http;

import com.google.common.collect.Lists;
import com.thenetcircle.service.data.hive.udf.commons.NamedThreadFactory;
import com.thenetcircle.service.data.hive.udf.commons.UDTFSelfForwardBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.FutureRequestExecutionService;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpRequestFutureTask;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.*;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;


@Description(name = UDTFAsyncHttpPost.NAME,
    value = "_FUNC_(url, offset, limit, timeout, headers, content) - send post to url with headers in timeout")
public class UDTFAsyncHttpPost extends UDTFSelfForwardBase {
    public static final String NAME = "async_http_post";

    private transient StringObjectInspector urlInsp;

    private int offset;
    private int limit;
    private int timeout;
    private transient RequestConfig rc;

    private transient MapObjectInspector headersInsp;

    private transient StringObjectInspector contentInsp;

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];

    private final int coreNum = Runtime.getRuntime().availableProcessors();

    private transient ThreadPoolExecutor threadPoolExecutor;
    private final int pageSize = 200;

    private transient ConcurrentLinkedQueue<Object[]> resultQueue = new ConcurrentLinkedQueue<>();

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(NAME, args, 6, 6);

        checkArgPrimitive(NAME, args, 0);
        this.urlInsp = (StringObjectInspector) args[0];

        if (args.length > 1) {
            checkArgPrimitive(NAME, args, 1);
            timeout = Optional.ofNullable(getConstantIntValue(NAME, args, 1)).orElse(0);
            rc = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
        }

        if (args.length > 2) {
            checkArgPrimitive(NAME, args, 2);
            offset = Optional.ofNullable(getConstantIntValue(NAME, args, 2)).orElse(0);
        }

        if (args.length > 3) {
            checkArgPrimitive(NAME, args, 3);
            limit = Optional.ofNullable(getConstantIntValue(NAME, args, 3)).orElse(10000);
        }

        //headers
        if (args.length > 4) {
            ObjectInspector headerInsp = args[4];
            if (headerInsp instanceof WritableVoidObjectInspector) {
                headersInsp = null;
            } else {
                if (!(headerInsp instanceof MapObjectInspector)) {
                    throw new UDFArgumentTypeException(4, "header parameter must be map<string, object> or null:\n\t" + args[2]);
                }
                MapObjectInspector moi = (MapObjectInspector) headerInsp;
                if (!(moi.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
                    throw new UDFArgumentTypeException(4, "header parameter must be map<string, object>");
                }
                headersInsp = moi;
            }
        }

        if (args.length > 5) {
            checkArgGroups(NAME, args, 5, inputTypes, STRING_GROUP);
            ObjectInspector contentObj = args[5];
            if (!(args[0] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(0, "content must be string");
            }
            this.contentInsp = (StringObjectInspector) contentObj;
        }

        if(null == threadPoolExecutor){
            threadPoolExecutor = new ThreadPoolExecutor(coreNum, coreNum * 2, 8, TimeUnit.SECONDS,
                    new LinkedBlockingDeque<>(200), new NamedThreadFactory("async_http_post"));
        }

        return RESULT_TYPE;
    }


    @Override
    public void process(Object[] args) throws HiveException {

        int start = 1;
        String urlStr = this.urlInsp.getPrimitiveJavaObject(args[start]);
        if (StringUtils.isBlank(urlStr)) {
            forwardAction(runtimeErr("url is blank"), args[0]);
        }


        HttpPost post = new HttpPost();
        post.setConfig(rc);
        Header[] headers;

        if (args.length > start + 4 && args[start + 4] != null && headersInsp != null) {
            Map<?, ?> headersMap = headersInsp.getMap(args[start + 2]);
            headers = map2Headers(headersMap);
            post.setHeaders(headers);
        }

        if (args.length > start + 5) {
            String content = contentInsp.getPrimitiveJavaObject(args[start + 5]);
            try {
                post.setEntity(new StringEntity(content));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                forwardAction(runtimeErr("url is blank"), args[0]);
            }
        }


        FutureRequestExecutionService futureRequestExecutionService =
                new FutureRequestExecutionService(hc, threadPoolExecutor);

        List<String> urls = Lists.newLinkedList();
        //pageable
        int taskNum = limit % pageSize == 0 ? limit / pageSize : (limit / pageSize + 1);
        for (int i = 0; i < taskNum; i += pageSize) {
            urls.add(String.format(urlStr, i, pageSize));
        }
        for (String url : urls) {
            post.setURI(URI.create(url));
            HttpRequestFutureTask<Object[]> task = futureRequestExecutionService.execute(
                    post,
                    hcContext,
                    respHandler,
                    new HCCallback(){
                        @Override
                        public void completed(Object[] result) {
                            resultQueue.offer(result);
                        }

                        @Override
                        public void failed(Exception ex) {
                            resultQueue.offer(runtimeErr(ex.getMessage()));
                        }
                    });
        }
        /*&& !resultQueue.isEmpty()*/
        while(threadPoolExecutor.getTaskCount() > 0 ){
            Object[] pollResults = Optional.ofNullable(resultQueue.poll())
                                            .orElse(runtimeErr("nothing in queue yet"));
            forwardAction(pollResults, args[0]);
        }

    }


    private transient CloseableHttpClient hc =
        HttpClientBuilder
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
            .build();


    @Override
    public void close() throws HiveException {
        HttpHelper.close(hc);
        hc = null;
        threadPoolExecutor.shutdown();
    }

}
