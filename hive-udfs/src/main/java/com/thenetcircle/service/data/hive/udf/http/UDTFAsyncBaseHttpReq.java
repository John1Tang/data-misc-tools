package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import com.thenetcircle.service.data.hive.udf.commons.MiscUtils;
import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.FutureRequestExecutionService;
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


    protected MapredContext mapredCtx;

    @Override
    public void configure(MapredContext mapredContext) {
        mapredCtx = mapredContext;
    }

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
     *
     * @param argInsps
     * @throws UDFArgumentTypeException
     */
    abstract boolean setBody(ObjectInspector[] argInsps) throws UDFArgumentTypeException;

    protected Object cloneCtxObj(Object ctx) {
        if (!(ctx instanceof Writable)) {
            return ctx;
        }
        JobConf jobConf = this.mapredCtx.getJobConf();
        log.info("init >> null check {}", jobConf.toString());
        return WritableUtils.clone((Writable) ctx, jobConf);
    }

    /**
     * Give a set of arguments for the UDTF to process.
     *
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
        Object _ctx = cloneCtxObj(ctx);
        HttpHelper.getInstance().executeFutureReq(_ctx, httpBaseReq, resultQueue);


        long processCnt = processCounter.longValue();
        long forwardCnt = forwardCounter.longValue();

        FutureRequestExecutionService futureReqExecSvc = HttpHelper.getInstance().getFutureReqExecSvc();
        long completedTaskCnt = futureReqExecSvc.metrics().getSuccessfulConnectionCount();
        while (processCnt - completedTaskCnt > corePoolSize) {
            MiscUtils.easySleep(1000);
            log.info("\n\n -- #process -> #easySleep 1 sec -- thread pool is full! " +
                            "current index: {}, cloned: {}, process: {}, forward: {}, completedTaskCnt: {}\n\n",
                    ctx, _ctx, processCnt, forwardCnt, completedTaskCnt);
            completedTaskCnt = threadPoolExecutor.getCompletedTaskCount();
        }
        pollAndForward();
    }


    /**
     * getHttpBaseReq
     *
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
            log.info("\n\n -- close() -- waited 1 second but not result in queue yet! threadInfo: {}, processCnt:{}, forwardCnt: {}\n\n",
                    NetUtil.getNet().getRunInfo(), processCnt, forwardCnt);

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
