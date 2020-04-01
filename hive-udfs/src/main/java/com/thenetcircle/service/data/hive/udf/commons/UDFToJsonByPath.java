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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.Map;

import static com.thenetcircle.service.data.hive.udf.commons.Jsons.toJsonByPath;
import static java.lang.String.format;

@Description(name = "json_by_path",
        value = "_FUNC_(map) - Returns a json string, key can be a path 'a.b.c.e'")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFToJsonByPath extends GenericUDF {
    private transient MapObjectInspector mapInsp;
    private transient JavaStringObjectInspector strInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 1, 1);
        if (ObjectInspector.Category.MAP != args[0].getCategory()) {
            throw new UDFArgumentException("json parameter must be map");
        }
        mapInsp = (SettableMapObjectInspector) args[0];
        if (!(mapInsp.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("json parameter must be map<String, object>");
        }

        strInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return strInsp;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (ArrayUtils.isEmpty(args)) return "null";

        Map map = mapInsp.getMap(args[0]);
        if (MapUtils.isEmpty(map)) return "{}";

        try {
            return toJsonByPath(map);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("json(%s)", StringUtils.join(children, ", "));
    }
}