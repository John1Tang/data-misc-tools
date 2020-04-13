package com.thenetcircle.service.data.hive.udf.commons;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import static java.lang.String.format;

@Description(name = "test_throw",
        value = "_FUNC_(anything) - test for return an error msg")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class TestThrow extends GenericUDF {

    private transient JavaStringObjectInspector strInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        super.checkArgsSize(arguments, 1, 1);
        strInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return strInsp;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            String runInfo = NetUtil.getNet().getRunInfo();
            throw new Exception("Error on machine:" + runInfo);
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("test_throw(%s)", StringUtils.join(children, ", "));
    }
}
