package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

/**
 * @description:
 * @author: john
 * @created: 2020/05/20 16:54
 */
@Description(name = "mark_json_array",
        value = "_FUNC_(array_string, delimiter) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class UDFMarkJsonArray extends GenericUDF {

    public static final String NAME = "json_array";

    private transient StringObjectInspector strInsp;


    public static final PrimitiveObjectInspector RESULT_TYPE = javaStringObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 2, 2);
        checkArgPrimitive(args, 0);

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
    public Object evaluate(DeferredObject[] deferredObj) throws HiveException {
        if (deferredObj[0].get() == null || deferredObj[1].get() ==null) {
            return null;
        }
        try {
            JSONArray extractObject = new JSONArray(deferredObj[0].get().toString());
            return IntStream
                    .range(0,extractObject.length())
                    .mapToObj(i -> extractObject.getJSONObject(i).toString())
                    .collect(Collectors.joining(deferredObj[1].get().toString()));
        } catch (JSONException | NumberFormatException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            return new Object[]{-1, null, e.toString()};
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("mark_json_array(%s)", StringUtils.join(children, ", "));
    }


}
