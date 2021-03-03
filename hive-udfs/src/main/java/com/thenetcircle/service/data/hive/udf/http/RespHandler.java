package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

final class RespHandler implements ResponseHandler<Object[]> {

    private static Logger log = LoggerFactory.getLogger(RespHandler.class);

    private Object ctx;

    public RespHandler(Object ctx){

        log.info("init >> klassAddress: {}, threadInfo: {}, ctxParamAddr:{}, ctxParamType: {}, ctxParamVal: {}",
                System.identityHashCode(this), NetUtil.getNet().getRunInfo(),
                System.identityHashCode(ctx), ctx.getClass(), ctx);

        try {
            // copy value to avoid computing state
            if (ctx instanceof Writable) {
                JobConf jobConf = MapredContext.get().getJobConf();
                log.info("init >> null check {}", jobConf.toString());
                this.ctx = WritableUtils.clone((Writable) ctx, jobConf);
            } else {
                this.ctx = ctx;
            }

            log.info("init >> thisCtxAddr: {}, thisCtxType: {}, thisCtxVal: {}",
                    System.identityHashCode(this.ctx), this.ctx.getClass(), this.ctx);

        } catch (Exception e) {
            log.error("init >> {}", e.getMessage());
            e.printStackTrace();
            this.ctx = null;
        }
    }

    public Object getCtx() {
        return ctx;
    }

    @Override
    public Object[] handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {
        String resp = EntityUtils.toString(response.getEntity());
        log.info("handleResponse >> klassAddress: {}, threadInfo: {}, ctx: {}, handleResponse: {}",
                System.identityHashCode(this), NetUtil.getNet().getRunInfo(), getCtx(), resp.substring(0, 84));
        return new Object[]{
                response.getStatusLine().getStatusCode(),
                headers2Map(response.getAllHeaders()),
                resp,
                getCtx()};
    }
}
