package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;

@Description(name = "m_array",
        value = "_FUNC_(start, end, step) - Returns an array starts from 1st param ends with 2nd param")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFMakeArray extends GenericUDF {

    private transient LongObjectInspector longInspector = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    private static transient ListObjectInspector retInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[3];
    private ObjectInspectorConverters.Converter startConverter;
    private ObjectInspectorConverters.Converter endConverter;
    private ObjectInspectorConverters.Converter stepConverter;


    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 3, 3);

        checkArgGroups(args, 0, inputTypes, NUMERIC_GROUP);
        checkArgGroups(args, 1, inputTypes, NUMERIC_GROUP);
        checkArgGroups(args, 2, inputTypes, NUMERIC_GROUP);

        startConverter = ObjectInspectorConverters.getConverter(args[0], longInspector);
        endConverter = ObjectInspectorConverters.getConverter(args[1], longInspector);
        stepConverter = ObjectInspectorConverters.getConverter(args[2], longInspector);

        return retInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Long start = (Long) startConverter.convert(args[0].get());
        Long end = (Long) endConverter.convert(args[1].get());
        Long step = (Long) stepConverter.convert(args[2].get());
        return MiscUtils.range(start, end, step);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("m_array(%s)", StringUtils.join(children, ", "));
    }
}