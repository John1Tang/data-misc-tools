package com.thenetcircle.service.data.hive.udf.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @author john
 */
@Description(name = UDAFHttpPost.NAME, value = "_FUNC_(ctx, url, timeout, headers, content) - Returns the mean of a set of numbers")
public class UDAFHttpPost extends AbstractGenericUDAFResolver {

    static final String NAME = "agg_http_post";
    final static int PARAM_SIZE = 5;


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != PARAM_SIZE) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected. found " + parameters.length);
        }

        for (int i = 0; i < parameters.length; i++) {
            switch (parameters[i].getCategory()) {
                case PRIMITIVE:
                case STRUCT:
                case MAP:
                case LIST:
                    break;
                default:
                    throw new UDFArgumentTypeException(0,
                            "Only primitive, struct, list or map type arguments are accepted but "
                                    + parameters[i].getTypeName() + " was passed as parameter " + i + ".");
            }
        }

        return new UDAFHttpReqEvaluator(UDAFHttpReqEvaluator.HttpMethod.GET);
    }
}
