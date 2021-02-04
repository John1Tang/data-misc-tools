package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
import org.apache.commons.lang3.ObjectUtils;
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

//    private ThreadLocal<Object> tlCtx = new ThreadLocal<>();

    private final Object ctx;

    public RespHandler(Object ctx) {
        /*Object tlVal = tlCtx.get();
        if (tlVal == null) {
            tlVal = ctx;
            tlCtx.set(tlVal);
        }*/
        this.ctx = ObjectUtils.clone(ctx);
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
