package com.thenetcircle.service.data.hive.udf.commons;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.*;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.*;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;

@Description(name = "json_destruct2", value = "_FUNC_(jsonString) - returns array<struct<path,type,val>>")
public class UDFJsonDestruct2 extends GenericUDF {
    private transient StringObjectInspector strInsp;

    public static final List<String[]> NULL_RESULT = new LinkedList<>();

    private ListObjectInspector RESULT_TYPE = getStandardListObjectInspector(getStruct());

    private StructObjectInspector getStruct(){
        ArrayList<String> structFieldNames = Lists.newArrayListWithCapacity(3);
        ArrayList<ObjectInspector> structFieldObjectInspectors = Lists.newArrayListWithCapacity(3);

        structFieldNames.add("json_path");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        structFieldNames.add("json_type");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        structFieldNames.add("json_val");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);

        PrimitiveObjectInspector argOI;
        if (args[0] instanceof PrimitiveObjectInspector) {
            argOI = (PrimitiveObjectInspector) args[0];
        } else {
            throw new UDFArgumentException("json_destruct2 takes only primitive types. found " + args[0].getTypeName());
        }

        switch (argOI.getPrimitiveCategory()) {
            case VOID:
                return RESULT_TYPE;
            case STRING:
            case CHAR:
            case VARCHAR:
                break;
            default:
                throw new UDFArgumentException("json_destruct2 takes only STRING/CHAR/VARCHAR types. Found " + argOI.getPrimitiveCategory());
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

        return Jsons.decomposeWithType(jsonStr);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("json_destruct2(%s", StringUtils.join(children, ", "));
    }
}