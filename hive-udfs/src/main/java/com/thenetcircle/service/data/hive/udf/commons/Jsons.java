package com.thenetcircle.service.data.hive.udf.commons;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;

public class Jsons {

    public static ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.configure(SerializationFeature.INDENT_OUTPUT, false);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        MAPPER.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, false);
    }

    public static String toJsonByPath(Map<String, Object> obj) throws JsonProcessingException {
        if (MapUtils.isEmpty(obj)) return "{}";

        ObjectNode reNode = new ObjectNode(MAPPER.getNodeFactory());

        for (Map.Entry<String, Object> en : obj.entrySet()) {
            String pathStr = en.getKey();
            if (StringUtils.isBlank(pathStr)) continue;

            String[] paths = StringUtils.split(pathStr, '.');
            if (ArrayUtils.isEmpty(paths)) continue;

            ObjectNode node = reNode;
            for (int i = 0, j = paths.length; i < j - 1; i++) {
                String path = paths[i];
                JsonNode on = node.get(path);
                if (!(on instanceof ObjectNode)) {
                    on = new ObjectNode(MAPPER.getNodeFactory());
                    node.set(path, on);
                }
                node = (ObjectNode) on;
            }
            node.putPOJO(paths[paths.length - 1], en.getValue());
        }
        return MAPPER.writeValueAsString(reNode);
    }

    public static Map<String, String> decompose(String jsonStr) {
        if (StringUtils.isBlank(jsonStr)) return new HashMap<>();
        try {
            Map<String, String> map = new HashMap<>();
            JsonNode tree = MAPPER.readTree(jsonStr);

            List<Pair<String, String>> pairList = decompose(tree, "$");
            for (Pair<String, String> pair : pairList) {
                map.put(pair.getKey(), pair.getValue());
            }

            return map;
        } catch (IOException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    private static List<Pair<String, String>> decompose(JsonNode json, String prefix) {
        if (json == null || json.isEmpty(MAPPER.getSerializerProvider())) return null;

        if (!(json instanceof ContainerNode)) return Collections.singletonList(Pair.of(prefix, json.asText()));

        LinkedList<Pair<String, String>> pairList = new LinkedList<>();

        if (json instanceof ArrayNode) {
            ArrayNode arrayNode = (ArrayNode) json;
            for (int idx = 0, size = arrayNode.size(); idx < size; idx++) {
                JsonNode jn = arrayNode.get(idx);
                String path = prefix + "[" + idx + "]";
                if (jn instanceof ValueNode) {
                    pairList.push(Pair.of(path, jn.asText()));
                } else {
                    List<Pair<String, String>> subNodePairList = decompose(jn, path);
                    if (subNodePairList != null)
                        pairList.addAll(subNodePairList);
                }
            }
        } else if (json instanceof ObjectNode) {
            ObjectNode objNode = (ObjectNode) json;
            for (Iterator<Map.Entry<String, JsonNode>> fdIter = objNode.fields(); fdIter.hasNext(); ) {
                Map.Entry<String, JsonNode> fd = fdIter.next();
                JsonNode jn = fd.getValue();
                String path = prefix + "." + fd.getKey();
                if (jn instanceof ValueNode) {
                    pairList.push(Pair.of(path, jn.asText()));
                } else {
                    List<Pair<String, String>> subNodePairList = decompose(jn, path);
                    if (subNodePairList != null)
                        pairList.addAll(subNodePairList);
                }
            }
        }

        return pairList;
    }
}