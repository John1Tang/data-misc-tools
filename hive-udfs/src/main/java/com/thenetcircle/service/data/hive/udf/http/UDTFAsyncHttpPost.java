package com.thenetcircle.service.data.hive.udf.http;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.http.client.methods.HttpRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author john
 */
@Description(name = UDTFAsyncHttpPost.NAME,
        value = "_FUNC_(ctx, url, timeout, headers, content, coreSize) - send post to url with headers in timeout")
public class UDTFAsyncHttpPost extends UDTFAsyncBaseHttpReq {

    static final String NAME = "a_http_post";

    private static Logger log = LoggerFactory.getLogger(UDTFAsyncHttpPost.class);

    public UDTFAsyncHttpPost() {
        super.NAME = NAME;
    }

    @Override
    boolean setBody(ObjectInspector[] argInsps) throws UDFArgumentTypeException {
        HttpHelper.getInstance().setContent(argInsps, 4, NAME);
        return true;
    }

    @Override
    HttpRequestBase getHttpBaseReq(Object[] args, int start) throws HiveException {
        return HttpHelper.getInstance().setHttpPost(args, start + 1, start + 3, start + 4);
    }
}
