package com.thenetcircle.service.data.hive.udf.http;

import com.google.common.collect.Lists;
import com.thenetcircle.service.data.hive.udf.commons.MiscUtils;
import com.thenetcircle.service.data.hive.udf.commons.NamedThreadFactory;
import com.thenetcircle.service.data.hive.udf.commons.UDTFSelfForwardBase;
import org.apache.commons.lang.ObjectUtils;
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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.FutureRequestExecutionService;
import org.apache.http.impl.client.HttpRequestFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.*;


/**
 * @author john
 */
@Description(name = UDTFAsyncHttpPost.NAME,
    value = "_FUNC_(url, timeout, headers, content, coreSize) - send post to url with headers in timeout")
public class UDTFAsyncHttpPost extends UDTFSelfForwardBase {

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

    private transient ThreadPoolExecutor threadPoolExecutor;

    private transient static Object offset;

    private transient ConcurrentLinkedQueue<Object[]> resultQueue = new ConcurrentLinkedQueue<>();
    private HCCallback hcCallback;

    private transient CloseableHttpClient hc = createHc();

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(NAME, args, 4,  5);

        checkArgPrimitive(NAME, args, 0);
        this.urlInsp = (StringObjectInspector) args[0];

        setTimeout(args, 1);

        setReqHeader(args, 2);

        setContent(args, 3);

        setCoreSize(args, 4);


        if(null == threadPoolExecutor){
            threadPoolExecutor = new ThreadPoolExecutor(coreSize, coreSize * 2, 8, TimeUnit.SECONDS,
                    new LinkedBlockingDeque<>(1000), new NamedThreadFactory(NAME));
        }

        hcCallback = new HCCallback() {
            @Override
            public void completed(Object[] result) {
                log.info(Thread.currentThread().getName() + Arrays.toString(result));
                resultQueue.offer(result);
            }

            @Override
            public void failed(Exception ex) {
                resultQueue.offer(runtimeErr(ex.getMessage()));
            }

            @Override
            public void cancelled() {
                resultQueue.offer(runtimeErr("task cancelled"));
            }
        };

        return RESULT_TYPE;
    }


    @Override
    public void process(Object[] args) throws HiveException {

        log.info("--- start UDTFAsyncHttpPost process ---");
        int start = 0;

        HttpPost httpPost = setHttpPost(args, start + 1, start + 3, start + 4);

        if (null == hc) {
            hc = createHc();
        }

        FutureRequestExecutionService futureRequestExecutionService =
                new FutureRequestExecutionService(hc, threadPoolExecutor);

        HttpRequestFutureTask<Object[]> task = futureRequestExecutionService.execute(
                httpPost,
                hcContext,
                respHandler,
                hcCallback);

        for (int activeThreadCnt = threadPoolExecutor.getActiveCount();
         activeThreadCnt > 0 &&  threadPoolExecutor.getQueue().size() > 0;
             activeThreadCnt = threadPoolExecutor.getActiveCount()){
            log.info("poolActiveCount: {}, resultQueueSize: {}", threadPoolExecutor.getActiveCount(), resultQueue.size());
            if (!resultQueue.isEmpty()) {
                Object[] pollResults = resultQueue.poll();
                forwardAction(pollResults, args[0]);
                offset = args[0];

            } else {
                MiscUtils.easySleep(1000);
                log.info("nothing in the forward queue yet, there are {} active http threads under processing", activeThreadCnt);
            }

        }

    }


    @Override
    public void close() throws HiveException {

        log.info("\n\n\n close httpclient \n\n\n");
        HttpHelper.close(hc);
        hc = null;
        threadPoolExecutor.shutdown();
        threadPoolExecutor = null;
    }

    /**
     * generate a prototype HttpPost
     * @param args
     * @param idxHeader
     * @param idxContent
     * @return
     * @throws HiveException
     */
    private HttpPost setHttpPost(Object[] args, int idxUrl, int idxHeader, int idxContent) throws HiveException {

        String urlStr = this.urlInsp.getPrimitiveJavaObject(args[idxUrl]);
        if (StringUtils.isBlank(urlStr)) {
            forwardAction(runtimeErr("url is blank"), args[idxUrl]);
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
                forwardAction(runtimeErr("url is blank"), args[idxContent]);
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
