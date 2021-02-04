package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

final class RespHandler implements ResponseHandler<Object[]> {

    private static Logger log = LoggerFactory.getLogger(RespHandler.class);

    private Object ctx;

    private ObjectWritable ctxWritable;

    public RespHandler(ObjectWritable ctx){

        ctxWritable =  new ObjectWritable();
        try {
            ReflectionUtils.cloneWritableInto(this.ctxWritable, ctx);
            this.ctx = ObjectUtils.clone(ctxWritable.get());
        } catch (IOException e) {
            e.printStackTrace();
            this.ctx = null;
        }
        log.info("RespHandler::init >> klassAddress: {}, threadInfo: {}, ctxParamAddr:{}, ctxParamType: {}, ctxParamVal: {}, ctxCopy: {}",
                System.identityHashCode(this), NetUtil.getNet().getRunInfo(),
                System.identityHashCode(ctx), ctx.getClass(), ctx, System.identityHashCode(this.ctx));
    }

    public Object getCtx() {
        return ctx;
    }

    @Override
    public Object[] handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {
        String resp = EntityUtils.toString(response.getEntity());
//        log.info("ctx: {}, handleResponse: {}", tlCtx.get(), resp.substring(0, 84));
        log.info("handleResponse >> klassAddress: {}, threadInfo: {}, ctx: {}, handleResponse: {}",
                System.identityHashCode(this), NetUtil.getNet().getRunInfo(), getCtx(), resp.substring(0, 84));
        return new Object[]{
                response.getStatusLine().getStatusCode(),
                headers2Map(response.getAllHeaders()),
                resp ,
                getCtx()};
    }
}
