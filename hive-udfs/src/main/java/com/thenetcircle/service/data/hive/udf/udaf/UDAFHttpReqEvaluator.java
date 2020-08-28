package com.thenetcircle.service.data.hive.udf.udaf;

import com.google.common.collect.Lists;
import com.thenetcircle.service.data.hive.udf.UDFHelper;
import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableStringObjectInspector;

/**
 * @author john
 */
public class UDAFHttpReqEvaluator extends GenericUDAFEvaluator
        implements Serializable {


    private static final Logger log = LoggerFactory.getLogger(UDAFHttpReqEvaluator.class);

    /**
     * For PARTIAL1 and COMPLETE: ObjectInspectors for original data
     */
    public static ObjectInspector ctxInsp;
    public transient static final List<String> FIELD_NAMES = Arrays.asList("url", "timeout", "headers", "content");
    private static final transient StringObjectInspector urlInsp = writableStringObjectInspector;
    private static final transient IntObjectInspector timeoutInsp = writableIntObjectInspector;
    private static final transient MapObjectInspector headersInsp = getStandardMapObjectInspector(
            writableStringObjectInspector,
            writableStringObjectInspector);
    private static final transient StringObjectInspector contentInsp = writableStringObjectInspector;
    public transient static final List<ObjectInspector> FIELD_INSPECTORS = Arrays.asList(urlInsp, timeoutInsp, headersInsp, contentInsp);
    public StandardStructObjectInspector objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(FIELD_NAMES, FIELD_INSPECTORS);

    /**
     * For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
     * of objs)
      */
    private transient static ListObjectInspector loi;
    private transient static StructObjectInspector inputOI;

    private transient ListObjectInspector internalMergeOI;

    private ObjectInspectorConverters.Converter textConverter;


    // For PARTIAL1 and PARTIAL2
//    private Object[] partialResult;


    enum HttpMethod {
        GET, POST, PUT, DELETE
    }

    private HttpMethod httpMethod;

    public UDAFHttpReqEvaluator(HttpMethod httpMethod) {
        this.httpMethod = httpMethod;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {
        super.init(m, parameters);
        // init output object inspectors
        // The output of a partial aggregation is a list
        log.info("#init all params inspectors: {}, param 1: {}", Arrays.toString(parameters), parameters[0].getCategory());
        if (m == Mode.PARTIAL1) {
            log.info("#init enter Mode: {}", m);
            inputOI = UDFHelper.addCtxToFirstStructInsp(objectInspector, parameters[0]);
            log.info("#init inputOI inspector: {}", inputOI.getAllStructFieldRefs().stream().map(StructField::getFieldObjectInspector).collect(Collectors.toList()));
            return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);

        } else {
            log.info("#init enter Mode: {}", m);
            if (!(parameters[0] instanceof ListObjectInspector)) {
                //no map aggregation.
                return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
            } else {
                internalMergeOI = (ListObjectInspector) parameters[0];
                inputOI = (StructObjectInspector) internalMergeOI.getListElementObjectInspector();
                loi = (StandardListObjectInspector)
                        ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                return loi;
            }
        }
    }


    class HttpReqAggBuffer extends AbstractAggregationBuffer{

        private Collection<Collection<Object>> container = Lists.newLinkedList();

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new HttpReqAggBuffer();
    }


    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ((HttpReqAggBuffer)agg).container.clear();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

        log.info("iterate start on machine: {}", NetUtil.getNet().getRunInfo());
        assert (parameters.length != UDAFHttpPost.PARAM_SIZE);

        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;

        log.info("iterate 5 parameters: {}", Arrays.toString(parameters));
        // TODO: execute http request
        offerCollection(Arrays.asList(parameters), httpReqAggBuffer);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        log.info("terminatePartial start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;

        log.info("terminatePartial parameters:\n");

        httpReqAggBuffer.container.forEach(o ->
            log.info("terminatePartial collection: {}", Arrays.toString(o.toArray()))
        );
        List<Collection<Object>> ret = new ArrayList<>(httpReqAggBuffer.container.size());
        ret.addAll(httpReqAggBuffer.container);
        return ret;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        log.info("#merge start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;
        List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
        log.info("#merge partialResult classType: {}", partialResult.getClass());
        log.info("#merge partialResult[0] classType: {}", partialResult.get(0).getClass());
        if (partialResult != null) {
            for(Object i : partialResult) {
                offerCollection(i, httpReqAggBuffer);
            }
        }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        log.info("terminate start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;
        List<Collection<Object>> ret = new ArrayList<>(httpReqAggBuffer.container.size());
        ret.addAll(httpReqAggBuffer.container);
        return ret;
    }


    private void offerCollection(Object p, HttpReqAggBuffer httpReqAggBuffer) {
        ArrayList<Object> pCopy =  (ArrayList<Object>)ObjectInspectorUtils.copyToStandardObject(p, inputOI);
        log.info("#offerCollection pCopy:" + Arrays.toString(pCopy.toArray()));
        pCopy.stream().forEach( o -> log.info("#offerCollection classname: {}",o.getClass()));
        httpReqAggBuffer.container.add(pCopy);
    }


}
