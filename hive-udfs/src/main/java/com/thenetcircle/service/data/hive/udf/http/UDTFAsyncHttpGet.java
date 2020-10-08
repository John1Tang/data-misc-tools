package com.thenetcircle.service.data.hive.udf.http;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author john
 */
@Description(name = UDTFAsyncHttpGet.NAME,
        value = "_FUNC_(ctx, url, timeout, headers, coreSize) - send post to url with headers in timeout")
public class UDTFAsyncHttpGet extends UDTFAsyncBaseHttpReq {

    static final String NAME = "a_http_get";

    private static Logger log = LoggerFactory.getLogger(UDTFAsyncHttpGet.class);

    public UDTFAsyncHttpGet() {
        super.NAME = NAME;
    }

    @Override
    void setBody(ObjectInspector[] argInsps) throws UDFArgumentTypeException {
        // not need
    }

    @Override
    HttpRequestBase getHttpBaseReq(Object[] args, int start) throws HiveException {
        return HttpHelper.getInstance().setHttpGet(args, start + 1, start + 3);
    }
}
