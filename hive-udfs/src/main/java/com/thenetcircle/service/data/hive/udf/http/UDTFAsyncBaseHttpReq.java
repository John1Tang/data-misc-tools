package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import com.thenetcircle.service.data.hive.udf.commons.MiscUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.http.client.methods.HttpRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private ReadWriteLock lock = new ReentrantReadWriteLock();

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

    @Override
    public void process(Object[] args) throws HiveException {

        Object ctx = args[0];
        processCounter.accumulate(1);

        ThreadPoolExecutor threadPoolExecutor = HttpHelper.getInstance().getThreadPoolExecutor(NAME);
        log.info("\n--- start process --- \nwith ctx: {}\ncorePool size: {}, active: {}",
                ctx, threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getActiveCount());

        int start = 0;
        HttpRequestBase httpBaseReq = getHttpBaseReq(args, start);

        // forward in time
        executeFutureReq1((Writable)ctx, httpBaseReq);


        while (processCounter.longValue() - forwardCounter.longValue() > threadPoolExecutor.getCorePoolSize()) {
            MiscUtils.easySleep(1000);
            log.info("\n\n -- #process -> #easySleep 1 sec -- thread pool is full! " +
                            "current index: {}, process: {}, forward: {}, corePool size: {}\n\n",
                    ctx, processCounter.longValue(), forwardCounter.longValue(), threadPoolExecutor.getCorePoolSize());
        }
    }

    public void executeFutureReq1(Writable ctx, HttpRequestBase httpRequestBase) {
        log.info("submit url: {}, ctx: {}", httpRequestBase.getURI(), ctx);
        HttpHelper.getInstance().getFutureReqExecSvc().execute(
                httpRequestBase,
                HttpHelper.getHcContext(),
                new RespHandler(ctx),
                new IHttpClientCallback() {
                    @Override
                    public void completed(final Object[] result) {
                        try {
                            if (lock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
                                forward(result);
                                log.info("\n\n -- process() --going to forward nanoTime: {}, ctx: {} status: {}",
                                        System.nanoTime(), result[3], result[0]);

                            }
                        } catch (HiveException | InterruptedException e) {
                            e.printStackTrace();
                        }
                        finally {
                            lock.writeLock().unlock();
                        }
                        forwardCounter.increment();
                    }

                    @Override
                    public void failed(final Exception ex) {
                        try {
                            if (lock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
                                forward(runtimeErr(ctx, ex.getMessage()));
                            }
                        } catch (HiveException | InterruptedException e) {
                            e.printStackTrace();
                        }
                        finally {
                            lock.writeLock().unlock();
                        }
                        forwardCounter.increment();
                    }

                    @Override
                    public void cancelled() {
                        try {
                            if (lock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
                                forward(runtimeErr(ctx, "task cancelled"));
                            }
                        } catch (HiveException | InterruptedException e) {
                            e.printStackTrace();
                        }
                        finally {
                            lock.writeLock().unlock();
                        }
                        forwardCounter.increment();
                    }
                });
    }

    public static Object[] runtimeErr(Object ctx, String errMsg) {
        return new Object[]{-1, null, errMsg,ctx};
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

        while (processCounter.longValue() > forwardCounter.longValue()) {
            MiscUtils.easySleep(1000);
            log.info("\n\n -- close() -- waited 1 second but not result in queue yet! forward size: {}\n\n", forwardCounter.longValue());
        }

        log.info("\n\n\n close httpclient \naccepted {} records\nforwarded {} records\n\n\n", processCounter.get(), forwardCounter.longValue());
        log.info("\n FutureRequestExecutionMetrics: {}", HttpHelper.getInstance().getFutureReqExecSvc().metrics().toString());
        // this closure will close executorService & httpclient
        HttpHelper.getInstance().closeFutureReqExecSvc();
    }

}
