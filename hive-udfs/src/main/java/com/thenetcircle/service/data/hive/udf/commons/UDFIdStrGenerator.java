package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = false)
@Description(name = "id_str_generator", value = "_FUNC_() - returns string. Generates a global unique Id")
public class UDFIdStrGenerator extends GenericUDF {

    public static final String NAME = "id_generator";

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length > 0) {
            throw new UDFArgumentLengthException("The function " + NAME.toUpperCase() +
                    "requires no argument");
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return new Text(IdGenerator.generate());
    }

    @Override
    public String getDisplayString(String[] children) {
        return NAME;
    }

}
