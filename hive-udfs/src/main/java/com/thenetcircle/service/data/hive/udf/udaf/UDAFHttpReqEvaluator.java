package com.thenetcircle.service.data.hive.udf.udaf;

import com.thenetcircle.service.data.hive.udf.commons.NetUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * @author john
 */
public class UDAFHttpReqEvaluator extends GenericUDAFEvaluator
        implements Serializable {


    private static final Logger log = LoggerFactory.getLogger(UDAFHttpReqEvaluator.class);

    /**
     * For PARTIAL1 and COMPLETE: ObjectInspectors for original data
     */
    private transient ObjectInspector ctx;
    private transient StringObjectInspector url;
    private transient IntObjectInspector timeout;
    private transient MapObjectInspector headers;
    private transient StringObjectInspector content;

    /**
     * For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
     * of objs)
      */
    private transient StandardListObjectInspector loi;
    private transient StructObjectInspector inputOI;

    private transient ListObjectInspector internalMergeOI;

    private List<String> names = Arrays.asList("ctx", "url", "timeout", "headers", "content");

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
            List<ObjectInspector> inspectors = getObjectInspectors(parameters);
            return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);

        } else {
            if (!(parameters[0] instanceof ListObjectInspector)) {
                //no map aggregation.
                inputOI = (StructObjectInspector) parameters[0];
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

    private List<ObjectInspector> getObjectInspectors(ObjectInspector[] parameters) {
        ctx = parameters[0];
        url = (StringObjectInspector) parameters[1];
        timeout = (IntObjectInspector) parameters[2];
        headers = (MapObjectInspector) parameters[3];
        content = (StringObjectInspector) parameters[4];

        List<ObjectInspector> inspectors = Arrays.asList(ctx, url, timeout, headers, content);
        return inspectors;
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
        assert (parameters.length > 0);

        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;
        if (parameters.length == UDAFHttpPost.PARAM_SIZE) {
            log.info("iterate 5 parameters: {}", Arrays.toString(parameters));
            Object p1 = parameters[0];
            Object p2 = url.getPrimitiveJavaObject(parameters[1]);
            Object p3 = timeout.getPrimitiveJavaObject(parameters[2]);
            Object p4 = headers.getMap(parameters[3]);
            Object p5 = content.getPrimitiveJavaObject(parameters[4]);
            // TODO: execute http request

            offerCollection(new Object[]{p1, p2, p3, p4, p5}, httpReqAggBuffer);
        } else {
            log.info("iterate one parameter: {}", Arrays.toString(parameters));
            offerCollection((Object[]) parameters[0], httpReqAggBuffer);
        }

    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        log.info("terminatePartial start on machine: {}", NetUtil.getNet().getRunInfo());
        HttpReqAggBuffer httpReqAggBuffer = (HttpReqAggBuffer) agg;
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
        Object[] pCopy = (Object[]) ObjectInspectorUtils.copyToStandardObject(p,  this.internalMergeOI);
        log.info("offerCollection: pCopy:" + Arrays.toString(pCopy));
        httpReqAggBuffer.container.add(pCopy);
    }
}
