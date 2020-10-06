package com.thenetcircle.service.data.hive.udf.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Description(name="json_map",
        value = "_FUNC_(json) - Returns a map of key-value pairs from a JSON object")
public class JsonMapUDF extends GenericUDF {

    private StringObjectInspector stringInspector;

    private static final ObjectMapper om = new ObjectMapper();

    private static final TypeReference<HashMap<String, Object>> mapType = new TypeReference<HashMap<String, Object>>() {};

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());

            HashMap<String, Object> root = om.readValue(jsonString, mapType);

            return root.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        } catch(IOException jsonProc) {
            throw new HiveException(jsonProc);
        } catch (NullPointerException npe){
            return null;
        }

    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "json_split(" + arg0[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if(arguments.length != 1
                || ! arguments[0].getCategory().equals( ObjectInspector.Category.PRIMITIVE)
                || ((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Usage : json_split(jsonstring) ");
        }
        stringInspector = (StringObjectInspector) arguments[0];

        return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    }

}
