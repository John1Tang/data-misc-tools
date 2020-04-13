package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;


public class TestThrowTest {

    @Test
    public void evaluate() throws HiveException {
        TestThrow testThrow = new TestThrow();
        ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) testThrow.initialize(
                new ObjectInspector[] { input });
        Text forwards = new Text("hello");
        Object result = testThrow.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(forwards) });

        System.out.println(resultInspector.getPrimitiveJavaObject(result));
    }
}