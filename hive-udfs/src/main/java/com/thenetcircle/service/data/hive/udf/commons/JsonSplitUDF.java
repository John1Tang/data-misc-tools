package com.thenetcircle.service.data.hive.udf.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

@Description(name="json_split",
        value = "_FUNC_(json) - Returns a array of JSON strings from a JSON Array")
public class JsonSplitUDF extends GenericUDF {

    private Logger log = LoggerFactory.getLogger(JsonSplitUDF.class);
    private StringObjectInspector stringInspector;

    private static final ObjectMapper om = new ObjectMapper();
    private LongAdder longAdder = new LongAdder();


    public Object[] parseJsonStr(LongAdder longAdder, Object node) {
        longAdder.increment();
        if (node instanceof String) {
            return new Object[] {longAdder.intValue(), node};
        }
        try {
            return new Object[] {longAdder.intValue(), om.writeValueAsString(node) };
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new Object[] {};
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());

            ArrayList<Object> root = om.readValue(jsonString, ArrayList.class);

            longAdder.decrement();
            return root.stream().map(node -> parseJsonStr(longAdder, node))
                    .collect(Collectors.toCollection(ArrayList::new));

        } catch(IOException jsonProc) {
            throw new HiveException(jsonProc);
        } catch (NullPointerException npe){
            return null;
        }finally {
            log.info("finally done for evaluate {}", longAdder.intValue());
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

        ArrayList<String> outputColumns = new ArrayList<String>();
        outputColumns.add("row_id");
        outputColumns.add("json_string");

        ArrayList<ObjectInspector> outputTypes = new ArrayList<>();
        outputTypes.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        outputTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardListObjectInspector
                (ObjectInspectorFactory.getStandardStructObjectInspector( outputColumns, outputTypes));

    }
}
