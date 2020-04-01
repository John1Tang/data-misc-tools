package com.thenetcircle.service.data.hive.udf.commons;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

@Description(name = "user_agent",
        value = "_FUNC_(string) - Returns a struct contains(browser:string, device:string) v1")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFUserAgent extends GenericUDF {
    private transient StringObjectInspector strInsp;

    public static final StandardStructObjectInspector RESULT_TYPE = getStandardStructObjectInspector(
            asList("browser", "device"),
            asList(javaStringObjectInspector, javaStringObjectInspector));

    public static final Object[] NULL_RESULT = new Object[]{null, null};

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);

        PrimitiveObjectInspector argOI;
        if (args[0] instanceof PrimitiveObjectInspector) {
            argOI = (PrimitiveObjectInspector) args[0];
        } else {
            throw new UDFArgumentException("user_agent takes only primitive types. found "
                    + args[0].getTypeName());
        }

        switch (argOI.getPrimitiveCategory()) {
            case VOID:
                return RESULT_TYPE;
            case STRING:
            case CHAR:
            case VARCHAR:
                break;
            default:
                throw new UDFArgumentException("user_agent takes only STRING/CHAR/VARCHAR types. Found "
                        + argOI.getPrimitiveCategory());
        }

        this.strInsp = (StringObjectInspector) argOI;
        return RESULT_TYPE;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        DeferredObject arg0 = args[0];
        if (arg0 == null || this.strInsp == null) return NULL_RESULT;

        String useAgentStr = this.strInsp.getPrimitiveJavaObject(arg0.get());
        if (StringUtils.isBlank(useAgentStr)) {
            return NULL_RESULT;
        }

        UserAgent ua = UserAgent.parseUserAgentString(useAgentStr);
        return new Object[]{ua.getBrowser() + "_" + ua.getBrowserVersion(),
                ua.getOperatingSystem() != null ? ua.getOperatingSystem().toString() : null};
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("user_agent(%s)", StringUtils.join(children, ", "));
    }
}