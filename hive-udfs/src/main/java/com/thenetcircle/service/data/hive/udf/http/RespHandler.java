package com.thenetcircle.service.data.hive.udf.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

final class RespHandler implements ResponseHandler<ThreadLocal<Object[]>> {

    private transient ThreadLocal<Object> ctx;

    private transient ThreadLocal<Object[]> resp;

    public RespHandler(Object ctx) {
        this.ctx = new ThreadLocal<>();
        this.resp = new ThreadLocal<>();
        this.ctx.set(ctx);
    }

    @Override
    public ThreadLocal<Object[]> handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {
        resp.set(new Object[]{
                response.getStatusLine().getStatusCode(),
                headers2Map(response.getAllHeaders()),
                EntityUtils.toString(response.getEntity()) ,
                ctx.get()});
        return resp;
    }
}
