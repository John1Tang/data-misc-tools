package com.thenetcircle.service.data.hive.udf.http;

import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
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
        this.ctx = ctx;
    }


    @Override
    public Object[] handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {
        String resp = EntityUtils.toString(response.getEntity());
//        log.info("ctx: {}, handleResponse: {}", tlCtx.get(), resp.substring(0, 84));
        log.info("klassAddress: {}, threadInfo: {}, ctx: {}, handleResponse: {}",
                System.identityHashCode(this), NetUtil.getNet().getRunInfo(), ctx, resp.substring(0, 84));
        return new Object[]{
                response.getStatusLine().getStatusCode(),
                headers2Map(response.getAllHeaders()),
                resp ,
                ctx};
    }
}
