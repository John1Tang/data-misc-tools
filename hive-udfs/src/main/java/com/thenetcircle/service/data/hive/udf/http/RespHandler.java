package com.thenetcircle.service.data.hive.udf.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

final class RespHandler implements ResponseHandler<ThreadLocal<Object[]>> {

    private static Logger log = LoggerFactory.getLogger(RespHandler.class);

    private transient ThreadLocal<Object> ctx = new ThreadLocal<>();

    private transient ThreadLocal<Object[]> resp = new ThreadLocal<>();

    public RespHandler(Object ctx) {
        this.ctx.set(ctx);
    }



    @Override
    public ThreadLocal<Object[]> handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {
        log.info("RespHandler set  ctx:{}", ctx.get());
        resp.set(new Object[]{
                response.getStatusLine().getStatusCode(),
                headers2Map(response.getAllHeaders()),
                EntityUtils.toString(response.getEntity()) ,
                ctx.get()});
        return resp;
    }
}
