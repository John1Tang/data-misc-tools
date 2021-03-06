package com.thenetcircle.service.data.hive.udf.kafka;

import com.thenetcircle.service.data.hive.udf.UDFHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.thenetcircle.service.data.hive.udf.UDFHelper.deferedObj2Map;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.getConverter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.getStandardObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;


@Description(name = "kf_pull",
    value = "_FUNC_(map, start_date_str, end_date_str, topics...) - poll records from kafka by time windows and topics, it returns an array of structs with properties of t(topic), ct(creation time), k(key) and v(value), time format yyyy-[m]m-[d]d hh:mm:ss[.f...]")
@UDFType(deterministic = false, stateful = false, distinctLike = true)
public class UDFKafkaPull extends GenericUDF {

    private transient MapObjectInspector settingInsp = null;
    private transient KafkaConsumer<String, String> consumer = null;
    private transient StringObjectInspector topicInsp;

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;
    private transient StringObjectInspector startDateStrInsp = null;
    private transient StringObjectInspector endDateStrInsp = null;
    private transient StandardMapObjectInspector retInsp;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[args.length];
        checkArgsSize(args, 4, Integer.MAX_VALUE);
        {
            if (!(args[0] instanceof MapObjectInspector)) {
                throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + args[0]);
            }
            MapObjectInspector moi = (MapObjectInspector) args[0];
            if (!(moi.getMapKeyObjectInspector() instanceof StringObjectInspector && moi.getMapValueObjectInspector() instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(0, "Setting parameter must be map<String, String>:\n\t" + moi);
            }
            settingInsp = moi;
            retInsp = (StandardMapObjectInspector) getStandardObjectInspector(settingInsp, ObjectInspectorCopyOption.JAVA);
        }

        checkArgGroups(args, 1, inputTypes, STRING_GROUP);
        checkArgGroups(args, 2, inputTypes, STRING_GROUP);

        startDateStrInsp = (StringObjectInspector) args[1];
        endDateStrInsp = (StringObjectInspector) args[2];

        for (int i = 3, j = args.length; i < j; i++) {
            checkArgPrimitive(args, i);
            checkArgGroups(args, i, inputTypes, STRING_GROUP);
            if (!(args[i] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(0, "path parameter must be string:\n\t" + args[i]);
            }
        }
        topicInsp = (StringObjectInspector) args[2];

        return KafkaHelper.KAFKA_RECORD_LIST_INSP;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (ArrayUtils.isEmpty(args)) {
            return new Object[0];
        }

        Converter dateConverter = getConverter(
            startDateStrInsp,
            javaTimestampObjectInspector);

        Timestamp start = (Timestamp) dateConverter.convert(args[1].get());// UDFHelper.getDate("kf_pull", args[1], PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, dateConverter);
        dateConverter = getConverter(
            endDateStrInsp,
            javaTimestampObjectInspector);
        Timestamp end = (Timestamp) dateConverter.convert(args[2].get()); //UDFHelper.getDate("kf_pull", args[2], PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, dateConverter);

        if (start == null || end == null || start.equals(end) || start.after(end)) {
            System.out.printf("start: %s and end: %s are erroneous\n", start, end);
            return new Object[0];
        }

        Converter topicConverter = getConverter(topicInsp, javaStringObjectInspector);

        DeferredObject[] topicDeferredObjs = ArrayUtils.subarray(args, 3, args.length);
        String[] argTopics = Stream.of(topicDeferredObjs)
            .map(tpObj -> UDFHelper.deferedObjGet(tpObj, null))
            .map(topicConverter::convert)
            .filter(Objects::nonNull)
            .map(String::valueOf)
            .filter(StringUtils::isNotBlank)
            .map(String::trim)
            .toArray(String[]::new);

        Map<?, ?> cfgs = deferedObj2Map(args[0], settingInsp, retInsp);
        if (!cfgs.containsKey(BOOTSTRAP_SERVERS_CONFIG)
            || cfgs.get(BOOTSTRAP_SERVERS_CONFIG) == null
            || StringUtils.isBlank(String.valueOf(cfgs.get(BOOTSTRAP_SERVERS_CONFIG)))) {
            throw new UDFArgumentException(format("%s is missing or invalid ", BOOTSTRAP_SERVERS_CONFIG));
        }

        List<ConsumerRecord<String, String>> recList = KafkaHelper.poll(cfgs,
            1000,
            start.getTime(),
            end.getTime(),
            argTopics);
        return recList.stream().map(KafkaHelper::kfRecord2HiveStruct).toArray(Object[]::new);
    }

    @Override
    public void close() throws IOException {
        super.close();
        Optional.ofNullable(consumer).ifPresent(KafkaConsumer::close);
    }

    @Override
    public String getDisplayString(String[] children) {
        return format("kf_pull(%s)", StringUtils.join(children, ", "));
    }
}
