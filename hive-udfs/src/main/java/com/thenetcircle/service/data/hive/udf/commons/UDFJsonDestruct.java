package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

@Description(name = "json_destruct", value = "_FUNC_(jsonString) - returns a map with each field value and corresponding json path")
public class UDFJsonDestruct extends GenericUDF {
    private transient StringObjectInspector strInsp;

    public static final StandardMapObjectInspector RESULT_TYPE = getStandardMapObjectInspector(
            javaStringObjectInspector, javaStringObjectInspector);
    public static final Map<String, String> NULL_RESULT = new HashMap<>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);

        PrimitiveObjectInspector argOI;
        if (args[0] instanceof PrimitiveObjectInspector) {
            argOI = (PrimitiveObjectInspector) args[0];
        } else {
            throw new UDFArgumentException("user_agent takes only primitive types. found " + args[0].getTypeName());
        }

        switch (argOI.getPrimitiveCategory()) {
            case VOID:
                return RESULT_TYPE;
            case STRING:
            case CHAR:
            case VARCHAR:
                break;
            default:
                throw new UDFArgumentException("user_agent takes only STRING/CHAR/VARCHAR types. Found " + argOI.getPrimitiveCategory());
        }

        this.strInsp = (StringObjectInspector) argOI;
        return RESULT_TYPE;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        DeferredObject arg0 = args[0];
        if (arg0 == null || this.strInsp == null) {
            return NULL_RESULT;
        }

        String jsonStr = this.strInsp.getPrimitiveJavaObject(arg0.get());
        if (StringUtils.isBlank(jsonStr)) {
            return NULL_RESULT;
        }

        return Jsons.decompose(jsonStr);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("json_destruct(%s", StringUtils.join(children, ", "));
    }
}