package com.thenetcircle.service.data.hive.udf.commons;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


@Description(name = "json_rm_keys",
        value = "_FUNC_(string, array_string) - Receives a json string and a list of keys and returns the json " +
                "string without those keys")
@UDFType(deterministic = true, stateful = false, distinctLike = true)
public class UDFJsonStrRemoveKeys extends GenericUDF {
    private static transient final Logger log = LoggerFactory.getLogger(UDFJsonStrRemoveKeys.class);

    private transient StringObjectInspector strInsp;
    private transient StandardListObjectInspector listInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        checkArgsSize(args, 2, 2);
        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "parameter must be string:\n\t" + args[0]);
        }
        if (!(args[1] instanceof StandardListObjectInspector)) {
            throw new UDFArgumentTypeException(0, "parameter must be array[string]:\n\t" + args[1]);
        }
        this.strInsp = (StringObjectInspector) args[0];
        this.listInsp = (StandardListObjectInspector) args[1];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0] == null || this.strInsp == null || args[1] == null) return "null";

        String jsonStr = this.strInsp.getPrimitiveJavaObject(args[0].get());
        List<Text> keys = (List<Text>) this.listInsp.getList(args[1].get());

        if (StringUtils.isBlank(jsonStr)) {
            return "null";
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode tree = mapper.readTree(jsonStr);
            for (Text k: keys) {
                ((ObjectNode) tree).remove(k.toString());
            }
            return tree.toString();
        } catch (Exception e) {
            log.error(String.format("jsonStr: %s\nERROR msg: %s", jsonStr, e.getMessage()), e);
        }

        return "null";
    }

    @Override
    public String getDisplayString(String[] children) {
        return String.format("json_rm_keys(%s)", StringUtils.join(children, ", "));
    }
}
