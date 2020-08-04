package com.thenetcircle.service.data.hive.udf.commons;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgsSize;

public abstract class UDTFSelfForwardBase extends GenericUDTF {
    protected String funcName = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
        throws UDFArgumentException {

        checkArgsSize(funcName, argOIs, 1, 100);

        ObjectInspector[] _argOIs = ArrayUtils.subarray(argOIs, 1, argOIs.length);

        StructObjectInspector _retInsp = _initialize(_argOIs);

        return UDFHelper.addContextToStructInsp(_retInsp, argOIs[0]);
    }

    public abstract StructObjectInspector _initialize(ObjectInspector[] argOIs) throws UDFArgumentException;

    public void forwardAction(Object[] rest, Object first) throws HiveException {
        Object[] results = ArrayUtils.add(rest, first);
        forward(results);
    }
}
