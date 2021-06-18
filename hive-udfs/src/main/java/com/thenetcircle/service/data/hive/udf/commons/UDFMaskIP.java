package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


@Description(name = "mask_ip",
        value = "_FUNC_(string) - Receives an IP address (v4 or v6) and returns a masked version of it")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFMaskIP extends GenericUDF {
    private transient StringObjectInspector strInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);
        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "parameter must be string:\n\t" + args[0]);
        }
        this.strInsp = (StringObjectInspector) args[0];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0] == null || this.strInsp == null) return "null";

        String ipValue = this.strInsp.getPrimitiveJavaObject(args[0].get());
        if (StringUtils.isBlank(ipValue)) {
            return "null";
        }

        String[] ipV4Split = ipValue.split("\\.");
        if (ipV4Split.length == 4) {
            return String.format("%s.%s.%s.0", ipV4Split[0], ipV4Split[1], ipV4Split[2]);
        }

        String[] ipV6Split = ipValue.split(":");
        if (ipV6Split.length == 8) {
            return String.format("%s:%s:%s:%s:%s:%s:0000:0000", ipV6Split[0], ipV6Split[1], ipV6Split[2], ipV6Split[3], ipV6Split[4], ipV6Split[5]);
        }

        return "null";
    }

    @Override
    public String getDisplayString(String[] children) {
        return String.format("mask_ip(%s)", StringUtils.join(children, ", "));
    }
}