package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgPrimitive;
import static com.thenetcircle.service.data.hive.udf.UDFHelper.checkArgsSize;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

/**
 * @description:
 * @author: john
 * @created: 2020/05/20 16:54
 */
@Description(name = "json_array",
        value = "_FUNC_(array_string) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class UDFJsonAsArray extends UDTFExt {

    public static final String NAME = "json_array";

    private transient StringObjectInspector strInsp;

    public static final List<String> RESULT_FIELDS = Arrays.asList("json_obj");
    public static final List<ObjectInspector> RESULT_FIELD_INSPECTORS = Arrays.asList(javaStringObjectInspector);

    public static final StandardStructObjectInspector RESULT_TYPE = ObjectInspectorFactory.getStandardStructObjectInspector(
            RESULT_FIELDS,
            RESULT_FIELD_INSPECTORS);

    @Override
    public StructObjectInspector _initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(NAME, args, 1, 1);
        checkArgPrimitive(NAME, args, 0);

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
    public Object[] evaluate(Object[] _args, int start) throws HiveException {
        if (_args == null) {
            return null;
        }
        try {
            JSONArray extractObject = new JSONArray(_args);
            List<String> result = new ArrayList<>();
            for (int ii = 0; ii < extractObject.length(); ++ii) {
                result.add(extractObject.get(ii).toString());
            }
            return new Object[]{result.toArray()};
        } catch (JSONException | NumberFormatException e) {
            e.printStackTrace(SessionState.getConsole().getChildErrStream());
            return new Object[]{-1, null, e.toString()};
        }
    }

    @Override
    public void close() throws HiveException {

    }

}
