package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import com.thenetcircle.service.data.hive.udf.commons.MiscUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.http.client.methods.HttpRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgPrimitive;
import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgsSize;
import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.ASYNC_RESULT_TYPE;


/**
 * @author john
 */
public abstract class UDTFAsyncBaseHttpReq extends GenericUDTF {

    String NAME = "a_http_base";

    private static Logger log = LoggerFactory.getLogger(UDTFAsyncBaseHttpReq.class);

    private transient LongAccumulator processCounter = new LongAccumulator((x, y) -> x + y, 0);
    private transient LongAdder forwardCounter = new LongAdder();

    private transient ConcurrentLinkedQueue<Object[]> resultQueue = new ConcurrentLinkedQueue<>();

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argInsps) throws UDFArgumentException {
        checkArgsSize(NAME, argInsps, 4, 6);

        checkArgPrimitive(NAME, argInsps, 1);

        HttpHelper.getInstance().setUrl(argInsps, 1);
        HttpHelper.getInstance().setTimeout(argInsps, 2, NAME);
        HttpHelper.getInstance().setReqHeader(argInsps, 3);

        boolean hasBody = setBody(argInsps);

        HttpHelper.getInstance().setCoreSize(argInsps, 4 + (hasBody ? 1 : 0), NAME);

        HttpHelper.getInstance().getThreadPoolExecutor(NAME);

        return UDFHelper.addContextToStructInsp(ASYNC_RESULT_TYPE, argInsps[0]);
    }

    /**
     * set request body if required
     * @param argInsps
     * @throws UDFArgumentTypeException
     */
    abstract boolean setBody(ObjectInspector[] argInsps) throws UDFArgumentTypeException;

    /**
     * Give a set of arguments for the UDTF to process.
     * @param args – object array of arguments
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {

        Object ctx = args[0];
        processCounter.accumulate(1);

        ThreadPoolExecutor threadPoolExecutor = HttpHelper.getInstance().getThreadPoolExecutor(NAME);
        int corePoolSize = threadPoolExecutor.getCorePoolSize();
        int activeCnt = threadPoolExecutor.getActiveCount();
        log.info("\n--- start process --- \nwith ctxType:{}, ctx: {}\ncorePoolSize: {}, activeCnt: {}",
                ctx.getClass(), ctx, corePoolSize, activeCnt);

        int start = 0;
        HttpRequestBase httpBaseReq = getHttpBaseReq(args, start);

        // forward in time
        HttpHelper.getInstance().executeFutureReq(ctx, httpBaseReq, resultQueue);


        long processCnt = processCounter.longValue();
        long forwardCnt = forwardCounter.longValue();

        long completedTaskCnt = threadPoolExecutor.getCompletedTaskCount();
        while (processCnt - completedTaskCnt > corePoolSize) {
            MiscUtils.easySleep(1000);
            log.info("\n\n -- #process -> #easySleep 1 sec -- thread pool is full! " +
                            "current index: {}, process: {}, forward: {}, completedTaskCnt: {}\n\n",
                    ctx, processCnt, forwardCnt, completedTaskCnt);
            completedTaskCnt = threadPoolExecutor.getCompletedTaskCount();
        }
        pollAndForward();
    }


    /**
     * getHttpBaseReq
     * @param args
     * @param start
     * @return
     * @throws HiveException
     */
    abstract HttpRequestBase getHttpBaseReq(Object[] args, int start) throws HiveException;


    @Override
    public void close() throws HiveException {

        log.info("\n\n{} #close()", NAME);

        long processCnt = processCounter.longValue();
        long forwardCnt = forwardCounter.longValue();
        
        while (processCnt > forwardCnt) {
            MiscUtils.easySleep(1000);
            forwardCnt = forwardCounter.longValue();
            log.info("\n\n -- close() -- waited 1 second but not result in queue yet! forward size: {}\n\n", forwardCnt);

            pollAndForward();
        }

        pollAndForward();


        log.info("\n\n\n close httpclient \naccepted {} records\nforwarded {} records\n\n\n", processCounter.get(), forwardCounter.longValue());
        log.info("\n FutureRequestExecutionMetrics: {}", HttpHelper.getInstance().getFutureReqExecSvc().metrics().toString());
        // this closure will close executorService & httpclient
        HttpHelper.getInstance().closeFutureReqExecSvc();
    }

    public void pollAndForward() throws HiveException {
        while (!resultQueue.isEmpty()) {
            Object[] pullResult = resultQueue.poll();
            long forwardCnt = forwardCounter.longValue();
            log.info("\n\n -- pollAndForward() --going to forward ctx: {} status: {}, forward: {}",
                    pullResult[3], pullResult[0], forwardCnt);
            forwardCounter.increment();
            forward(pullResult);
        }
    }

}
