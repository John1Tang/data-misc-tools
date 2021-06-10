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

@UDFType()
@Description(name = "id_generator", value = "_FUNC_() - returns Long. Generates a global unique Id")
public class UDFIdGenerator extends GenericUDF {

    public static final String NAME = "id_generator";
    protected transient final LongWritable idVal = new LongWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length > 0) {
            throw new UDFArgumentLengthException("The function " + NAME.toUpperCase() +
                    "requires no argument");
        }
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        idVal.set(IdGenerator.nextId());
        return idVal;
    }

    @Override
    public String getDisplayString(String[] children) {
        return NAME;
    }

}
