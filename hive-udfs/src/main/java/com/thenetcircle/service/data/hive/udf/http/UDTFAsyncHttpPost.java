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

import static com.thenetcircle.service.data.hive.udf.UDFHelper.*;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.*;


/**
 * @author john
 */
@Description(name = UDTFAsyncHttpPost.NAME,
    value = "_FUNC_(url, timeout, headers, content) - send post to url with headers in timeout")
public class UDTFAsyncHttpPost extends UDTFSelfForwardBase {

    public static final String NAME = "async_http_post";

    private static Logger log = LoggerFactory.getLogger(UDTFAsyncHttpPost.class);

    private transient StringObjectInspector urlInsp;
    private int offset;
    private int limit;
    private int timeout;
    private int pageSize;

    private transient RequestConfig rc;

    private transient MapObjectInspector headersInsp;

    private transient StringObjectInspector contentInsp;

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];

    private final int coreNum = Runtime.getRuntime().availableProcessors();

    private transient ThreadPoolExecutor threadPoolExecutor;


    private transient ConcurrentLinkedQueue<Object[]> resultQueue = new ConcurrentLinkedQueue<>();
    private HCCallback hcCallback;

    private transient CloseableHttpClient hc = createHc();

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(NAME, args, 4,  7);

        checkArgPrimitive(NAME, args, 0);
        this.urlInsp = (StringObjectInspector) args[0];

        setTimeout(args, 1);

        setReqHeader(args, 2);

        setContent(args, 3);

        setOffset(args, 4);

        setLimit(args, 5);

        setPageSize(args, 6);

        if(null == threadPoolExecutor){
            threadPoolExecutor = new ThreadPoolExecutor(coreNum, coreNum * 2, 8, TimeUnit.SECONDS,
                    new LinkedBlockingDeque<>(1000), new NamedThreadFactory("async_http_post"));
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

        log.info("--- start process ---");
        int start = 0;
        String urlStr = this.urlInsp.getPrimitiveJavaObject(args[start + 1]);
        if (StringUtils.isBlank(urlStr)) {
            forwardAction(runtimeErr("url is blank"), args[start + 1]);
        }

        HttpPost post = setHttpPost(args, start + 3, start + 4);

        if (null == hc) {
            hc = createHc();
        }

        FutureRequestExecutionService futureRequestExecutionService =
                new FutureRequestExecutionService(hc, threadPoolExecutor);

        List<String> urls = Lists.newLinkedList();
        //pageable
        int taskNum = limit % pageSize == 0 ? limit / pageSize : (limit / pageSize + 1);
        for (int i = offset; i < taskNum * pageSize; i += pageSize) {
            urls.add(String.format(urlStr, i, pageSize));
        }
        for (String url : urls) {
            log.info("--- paging query url {} ---", url);
            HttpPost postClone = (HttpPost) ObjectUtils.clone(post);
            postClone.setURI(URI.create(url));

            HttpRequestFutureTask<Object[]> task = futureRequestExecutionService.execute(
                    postClone,
                    hcContext,
                    respHandler,
                    hcCallback);
        }

        for (int activeThreadCnt = threadPoolExecutor.getActiveCount();
             activeThreadCnt > 0 ;
             activeThreadCnt = threadPoolExecutor.getActiveCount()){

            // TODO
            // reset page
            //
            // sleep
            if (!resultQueue.isEmpty()) {
                Object[] pollResults = Optional.ofNullable(resultQueue.poll())
                        .orElse(runtimeErr("nothing in queue yet"));
                forwardAction(pollResults, args[0]);
                return;
            } else {
//                MiscUtils.easySleep(1000);
                log.info("nothing in queue yet, there are {} active http threads", activeThreadCnt);
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
    private HttpPost setHttpPost(Object[] args, int idxHeader, int idxContent) throws HiveException {
        HttpPost post = new HttpPost();
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

    private void setPageSize(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
            checkArgPrimitive(NAME, args, idx);
            pageSize = Optional.ofNullable(getConstantIntValue(NAME, args, idx)).orElse(10000);
        }
    }

    private void setLimit(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
            checkArgPrimitive(NAME, args, idx);
            limit = Optional.ofNullable(getConstantIntValue(NAME, args, idx)).orElse(10000);
        }
    }

    private void setOffset(ObjectInspector[] args, int idx) throws UDFArgumentTypeException {
        if (args.length > idx) {
            checkArgPrimitive(NAME, args, idx);
            offset = Optional.ofNullable(getConstantIntValue(NAME, args, idx)).orElse(0);
        }
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
