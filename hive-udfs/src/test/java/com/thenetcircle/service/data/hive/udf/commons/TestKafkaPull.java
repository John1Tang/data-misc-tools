package com.thenetcircle.service.data.hive.udf.commons;

import com.thenetcircle.service.data.hive.udf.kafka.UDFKafkaPull;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: miguel.c
 * @created: 2021/11/18 14:13
 */
public class TestKafkaPull {

    @Test
    public void testKfPullArgs() throws HiveException {

        UDFKafkaPull udf = new UDFKafkaPull();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(stringOI, stringOI);
        StandardListObjectInspector resultInspector = (StandardListObjectInspector) udf.initialize(
                new ObjectInspector[]{
                        mapOI, stringOI, stringOI, stringOI
                });

        Map<String, String> map = new HashMap<>();
        map.put("bootstrap.servers", "10.20.2.101:9093,10.20.2.102:9093,10.20.2.103:9093,10.20.2.104:9093,10.20.2.115:9093");
        map.put("heartbeat.interval.ms", "800");
        map.put("metadata.max.age.ms", "300000");
        map.put("isolation.level", "read_uncommitted");
        map.put("ssl.keymanager.algorithm", "SunX509");
        map.put("metrics.num.samples", "2");
        map.put("auto.commit.interval.ms", "1000");
        map.put("receive.buffer.bytes", "65536");
        map.put("metrics.sample.window.ms", "30000");
        map.put("auto.offset.reset", "latest");
        map.put("max.poll.interval.ms", "300000");
        map.put("sasl.login.refresh.window.factor", "0.8");
        map.put("sasl.kerberos.kinit.cmd", "/usr/bin/kinit");
        map.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        map.put("sasl.login.refresh.window.jitter", "0.05");
        map.put("max.poll.records", "500");
        map.put("ssl.protocol", "TLS");
        map.put("check.crcs", "true");
        map.put("sasl.mechanism", "GSSAPI");
        map.put("fetch.min.bytes", "1");
        map.put("ssl.truststore.type", "JKS");
        map.put("ssl.trustmanager.algorithm", "PKIX");
        map.put("exclude.internal.topics", "true");
        map.put("enable.auto.commit", "false");
        map.put("max.partition.fetch.bytes", "1048576");
        map.put("sasl.kerberos.ticket.renew.window.factor", "0.8");
        map.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        map.put("security.protocol", "PLAINTEXT");
        map.put("ssl.endpoint.identification.algorithm", "https");
        map.put("ssl.keystore.type", "JKS");
        map.put("send.buffer.bytes", "131072");
        map.put("fetch.max.wait.ms", "500");
        map.put("client.id", "");
        map.put("connections.max.idle.ms", "540000");
        map.put("reconnect.backoff.max.ms", "1000");
        map.put("reconnect.backoff.ms", "50");
        map.put("request.timeout.ms", "11001");
        map.put("internal.leave.group.on.close", "true");
        map.put("retry.backoff.ms", "100");
        map.put("session.timeout.ms", "11000");
        map.put("group.id", "hive-udf");
        map.put("sasl.kerberos.min.time.before.relogin", "60000");
        map.put("fetch.max.bytes", "52428800");
        map.put("sasl.kerberos.ticket.renew.jitter", "0.05");
        map.put("default.api.timeout.ms", "60000");
        map.put("metrics.recording.level", "INFO");

        Object result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(map),
                new DeferredJavaObject("2021-11-17 00:00:00"),
                new DeferredJavaObject("2021-11-17 00:01:00"),
                new DeferredJavaObject("canal-114-feti")
        });
        // TODO: Assert something on the result, ATM not working from localhost since kafka cluster is private
    }

}
