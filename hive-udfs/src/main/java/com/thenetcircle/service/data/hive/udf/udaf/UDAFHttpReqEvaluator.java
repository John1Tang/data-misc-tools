package com.thenetcircle.service.data.hive.udf.udaf;

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
import java.util.*;

import static com.thenetcircle.service.data.hive.udf.http.HttpHelper.ASYNC_RESULT_TYPE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

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
    private static transient StringObjectInspector urlInsp = javaStringObjectInspector;
    private static transient IntObjectInspector timeoutInsp = javaIntObjectInspector;
    private static transient MapObjectInspector headersInsp = getStandardMapObjectInspector(
            javaStringObjectInspector,
            javaStringObjectInspector);
    private static transient StringObjectInspector contentInsp = javaStringObjectInspector;
    public transient static final List<ObjectInspector> FIELD_INSPECTORS = Arrays.asList(urlInsp, timeoutInsp, headersInsp, contentInsp);
    public StandardStructObjectInspector objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(FIELD_NAMES, FIELD_INSPECTORS);

    /**
     * For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
     * of objs)
      */
    private transient static ListObjectInspector loi;
    private transient static StructObjectInspector inputOI;

    private transient ListObjectInspector internalMergeOI;


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
        if (m == Mode.PARTIAL1) {
            inputOI = UDFHelper.addCtxToFirstStructInsp(objectInspector, parameters[0]);
            return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);

        } else {
            log.info("init Mode: {}, all params inspectors: {}, param 1: {}", m, Arrays.toString(parameters), parameters[0].getCategory());
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

        private Collection<Object[]> container;

        public HttpReqAggBuffer() {
            container = new LinkedHashSet<>();
        }
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

        offerCollection(parameters, httpReqAggBuffer);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        log.info("terminatePartial start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;

        log.info("terminatePartial parameters: {}", Arrays.toString(httpReqAggBuffer.container.toArray()));
        List<Object[]> ret = new ArrayList<>(httpReqAggBuffer.container.size());
        ret.addAll(httpReqAggBuffer.container);
        return ret;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        log.info("merge start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;
        List<Object[]> partialResult = (List<Object[]>) internalMergeOI.getList(partial);
        if (partialResult != null) {
            for(Object[] i : partialResult) {
                offerCollection(i, httpReqAggBuffer);
            }
        }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        log.info("terminate start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;
        List<Object[]> ret = new ArrayList<>(httpReqAggBuffer.container.size());
        ret.addAll(httpReqAggBuffer.container);
        return ret;
    }


    private void offerCollection(Object[] p, HttpReqAggBuffer httpReqAggBuffer) {
        log.info("offerCollection: object:" + Arrays.toString(p));
        // FIXME NullPointerException
        Object[] pCopy =  ((ArrayList<Object>)ObjectInspectorUtils.copyToStandardObject(p, inputOI)).toArray();
        log.info("offerCollection: pCopy:" + Arrays.toString(pCopy));
        httpReqAggBuffer.container.add(pCopy);
    }
}
