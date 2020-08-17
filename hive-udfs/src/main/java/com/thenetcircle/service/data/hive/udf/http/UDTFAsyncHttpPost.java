package com.thenetcircle.service.data.hive.udf.http;

import com.google.common.collect.Lists;
import com.thenetcircle.service.data.hive.udf.UDFHelper;
import com.thenetcircle.service.data.hive.udf.commons.MiscUtils;
import com.thenetcircle.service.data.hive.udf.commons.NamedThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.FutureRequestExecutionService;
import org.apache.http.impl.client.HttpRequestFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.*;


/**
 * @author john
 */
@Description(name = UDTFAsyncHttpPost.NAME,
        value = "_FUNC_(ctx, url, timeout, headers, content, coreSize) - send post to url with headers in timeout")
public class UDTFAsyncHttpPost extends GenericUDTF {

    public static final String NAME = "a_http_post";

    private static Logger log = LoggerFactory.getLogger(UDTFAsyncHttpPost.class);

    private transient StringObjectInspector urlInsp;
    private int timeout;
    private int coreSize;

    private transient RequestConfig rc;

    private transient MapObjectInspector headersInsp;

    private transient StringObjectInspector contentInsp;

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];

    private transient final int coreNum = Runtime.getRuntime().availableProcessors();

    private transient LongAccumulator processCounter = new LongAccumulator((x, y) -> x + y, 0);
    private transient LongAdder forwardCounter = new LongAdder();
    private transient LongAdder batchCounter = new LongAdder();

    private transient ConcurrentLinkedQueue<Object[]> resultQueue = new ConcurrentLinkedQueue<>();
    private IHttpClientCallback IHttpClientCallback;

    private transient CloseableHttpClient hc = createHc();
    private transient volatile ThreadPoolExecutor threadPoolExecutor;
    private transient FutureRequestExecutionService futureRequestExecutionService;

    private List<CompletableFuture<Void>> futureList = Lists.newLinkedList();

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argInsps) throws UDFArgumentException {
        checkArgsSize(NAME, argInsps, 5, 6);

        checkArgPrimitive(NAME, argInsps, 1);
        this.urlInsp = (StringObjectInspector) argInsps[1];

        setTimeout(argInsps, 2);

        setReqHeader(argInsps, 3);

        setContent(argInsps, 4);

        setCoreSize(argInsps, 5);


        threadPoolExecutor = getThreadPoolExecutor();

        IHttpClientCallback = new IHttpClientCallback() {
            @Override
            public void completed(Object[] result) {
                log.info(Thread.currentThread().getName() + Arrays.toString(result));
                resultQueue.offer(result);
            }

            @Override
            public void failed(Exception ex) {
                resultQueue.offer(runtimeErr(0, ex.getMessage()));
            }

            @Override
            public void cancelled() {
                resultQueue.offer(runtimeErr(0, "task cancelled"));
            }
        };

        return UDFHelper.addContextToStructInsp(ASYNC_RESULT_TYPE, argInsps[0]);
    }

    private ThreadPoolExecutor getThreadPoolExecutor() {
        if (null == threadPoolExecutor) {
            synchronized (this) {
                if (null == threadPoolExecutor) {
                    threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(coreSize * 2, new NamedThreadFactory(NAME));
                }
            }
        }
        return threadPoolExecutor;
    }


    @Override
    public void process(Object[] args) throws HiveException {

        Object ctx = args[0];
        processCounter.accumulate(1);
        log.info("\n--- start UDTFAsyncHttpPost process --- \nwith ctx: {}\n", ctx);
        int start = 0;

        HttpPost httpPost = setHttpPost(args, start + 1, start + 3, start + 4);

        if (null == futureRequestExecutionService){
            synchronized (this) {
                if (null == futureRequestExecutionService) {
                    futureRequestExecutionService = new FutureRequestExecutionService(hc, threadPoolExecutor);
                }
            }
        }

        HttpRequestFutureTask<Object[]> task = futureRequestExecutionService.execute(
                httpPost,
                hcContext,
                new RespHandler(ctx),
                new IHttpClientCallback() {
                    @Override
                    public void completed(Object[] result) {
                        log.info(Thread.currentThread().getName() + Arrays.toString(result));
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


        while (threadPoolExecutor.getActiveCount() == threadPoolExecutor.getMaximumPoolSize()) {
            log.info("\n\n -- process() -- thread pool is full! current index{}, forward: {}\n\n", ctx, forwardCounter.longValue());

            while (resultQueue.isEmpty()) {
                MiscUtils.easySleep(1000);
                log.info("\n\n -- process().easySleep -- thread pool is full! current index{}, forward: {}\n\n", ctx, forwardCounter.longValue());
            }

            // limit coming http request
            while (processCounter.longValue() > threadPoolExecutor.getPoolSize()
                    && processCounter.get() - forwardCounter.longValue() < threadPoolExecutor.getCorePoolSize()){
                MiscUtils.easySleep(1000);
                log.info("\n\n -- process().easySleep -- thread pool is full! current index{}, forward: {}\n\n", ctx, forwardCounter.longValue());
                if (!resultQueue.isEmpty() && resultQueue.size() > threadPoolExecutor.getCorePoolSize()){
                    break;
                }
            }

            while (!resultQueue.isEmpty()) {
                Object[] pullResult = resultQueue.poll();
                log.info("\n\n -- process() --going to forward ctx: {} status: {}", pullResult[3], pullResult[0]);
                forwardCounter.increment();
                forward(pullResult);
            }
            return;
        }

    }


    @Override
    public void close() throws HiveException {

        log.info("\n\nUDTFAsyncHttpPost.close()");
        
        while (threadPoolExecutor.getActiveCount() > 0) {
            while (resultQueue.isEmpty()) {
                MiscUtils.easySleep(1000);
                log.info("\n\n -- close() -- waited 1 second but not result in queue yet! forward size: {}\n\n", forwardCounter.longValue());
            }
            Object[] pullResult = resultQueue.poll();
            log.info("\n\n -- close() -- going to forward key: {} status: {}", pullResult[3], pullResult[0]);
            forwardCounter.increment();
            forward(pullResult);
            log.info("\n\n -- close() -- forward size: {}, process size: {}", forwardCounter.longValue(), processCounter.get());
        }

        while (!resultQueue.isEmpty()) {
            Object[] pullResult = resultQueue.poll();
            log.info("\n\n -- close() -- going to forward key: {} status: {}", pullResult[3], pullResult[0]);
            forwardCounter.increment();
            forward(pullResult);
        }

        log.info("\n\n\n close httpclient \naccepted {} records\nforwarded {} records\n\n\n", processCounter.get(), forwardCounter.longValue());
        HttpHelper.close(hc);
        hc = null;
        threadPoolExecutor.shutdown();
        threadPoolExecutor = null;
        try {
            futureRequestExecutionService.close();
        } catch (IOException e) {
            log.error("fail to close futureRequestExecutionService...");
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
    private HttpPost setHttpPost(Object[] args, int idxUrl, int idxHeader, int idxContent) throws HiveException {

        String urlStr = this.urlInsp.getPrimitiveJavaObject(args[idxUrl]);
        if (StringUtils.isBlank(urlStr)) {
            forward(runtimeErr(0, "url is blank"));
        }

        HttpPost post = new HttpPost(urlStr);
        post.setConfig(rc);
        Header[] headers;

        if (args.length > idxHeader && args[idxHeader] != null && headersInsp != null) {
            Map<?, ?> headersMap = headersInsp.getMap(args[idxHeader]);
            headers = map2Headers(headersMap);
            post.setHeaders(headers);
        }

        if (args.length > idxContent) {
            String content = contentInsp.getPrimitiveJavaObject(args[idxContent]);
            try {
                post.setEntity(new StringEntity(content));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                forward(runtimeErr(0, "url is blank"));
            }
        }
        return post;
    }

    private void setCoreSize(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
            checkArgPrimitive(NAME, args, idx);
            coreSize = Optional.ofNullable(getConstantIntValue(NAME, args, idx)).orElse(coreNum);
            if (coreSize < 1 || coreSize > coreNum) {
                coreSize = coreNum;
            }
            return;
        }
        coreSize = coreNum;
    }


    private void setContent(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
            checkArgPrimitive(NAME, args, idx);
            ObjectInspector contentObj = args[idx];
            if (!(args[idx] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(idx, "content must be string");
            }
            this.contentInsp = (StringObjectInspector) contentObj;
        }
    }

    private void setTimeout(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
            checkArgPrimitive(NAME, args, idx);
            timeout = Optional.ofNullable(getConstantIntValue(NAME, args, idx)).orElse(0);
            rc = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
        }
    }

    private void setReqHeader(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
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
    }


}
