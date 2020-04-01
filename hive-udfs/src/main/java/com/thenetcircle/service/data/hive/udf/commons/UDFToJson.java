package com.thenetcircle.service.data.hive.udf.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.structToMap;
import static com.thenetcircle.service.data.hive.udf.commons.Jsons.MAPPER;
import static java.lang.String.format;

@Description(name = "s_json",
        value = "_FUNC_(struct) - Returns a json string")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFToJson extends GenericUDF {
    private transient StandardStructObjectInspector structInsp;
    private transient JavaStringObjectInspector strInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);
        if (ObjectInspector.Category.STRUCT != args[0].getCategory()) {
            throw new UDFArgumentException("json parameter must be named_struct");
        }

        structInsp = (StandardStructObjectInspector) args[0];
        strInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return strInsp;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (ArrayUtils.isEmpty(args)) return "null";

        Map map = structToMap(structInsp, args[0].get());
        if (MapUtils.isEmpty(map)) return "{}";

        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("s_json(%s)", StringUtils.join(children, ", "));
    }
}