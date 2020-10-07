package com.thenetcircle.service.data.hive.udf.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.headers2Map;

final class RespHandler implements ResponseHandler<Object[]> {

    private transient Object ctx;

    public RespHandler() {
    }

    public RespHandler(Object ctx) {
        this.ctx = ctx;
    }

    @Override
    public Object[] handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {
        return new Object[]{
                response.getStatusLine().getStatusCode(),
                headers2Map(response.getAllHeaders()),
                EntityUtils.toString(response.getEntity()) ,
                ctx};
    }
}
