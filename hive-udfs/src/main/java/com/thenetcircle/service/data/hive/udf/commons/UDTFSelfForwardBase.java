package com.thenetcircle.service.data.hive.udf.commons;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgsSize;

/**
 * @author john
 */
public abstract class UDTFSelfForwardBase extends GenericUDTF {
    protected String funcName = null;

    private Logger log = LoggerFactory.getLogger(UDTFSelfForwardBase.class);

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
        throws UDFArgumentException {

        checkArgsSize(funcName, argOIs, 1, 100);

        ObjectInspector[] _argOIs = ArrayUtils.subarray(argOIs, 1, argOIs.length);

        StructObjectInspector _retInsp = _initialize(_argOIs);

        return UDFHelper.addContextToStructInsp(_retInsp, argOIs[0]);
    }

    public abstract StructObjectInspector _initialize(ObjectInspector[] argOIs) throws UDFArgumentException;

    @Override
    public void process(Object[] o) throws HiveException {
        log.info("--- UDTFSelfForwardBase process start ");
    }

    public void forwardAction(Object[] rest, Object first) throws HiveException {
        Object[] results = ArrayUtils.add(rest, first);
        forward(results);
    }
}
