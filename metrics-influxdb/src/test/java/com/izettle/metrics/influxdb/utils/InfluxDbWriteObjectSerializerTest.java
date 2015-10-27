package com.izettle.metrics.influxdb.utils;

import static com.izettle.metrics.influxdb.utils.InfluxDbWriteObjectSerializer.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.izettle.metrics.influxdb.data.InfluxDbPoint;
import com.izettle.metrics.influxdb.data.InfluxDbWriteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class InfluxDbWriteObjectSerializerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void init() {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        final SimpleModule module = new SimpleModule("SimpleModule", new Version(1, 0, 0, null, null, null));
        module.addSerializer(Map.class, new MapSerializer());
        objectMapper.registerModule(module);
    }

    @Test
    public void serializeStringMap() throws Exception {
        Map<String, String> testMap = new HashMap<String, String>();
        testMap.put("key", "value");
        final String json = objectMapper.writeValueAsString(testMap);

        assertThat(json).isNotEmpty();
        assertThat(json).isEqualTo("{\"key\":\"value\"}");
    }

    @Test
    public void serializeStringObjectMap() throws Exception {
        Map<String, Integer> testMap = new HashMap<String, Integer>();
        testMap.put("intObject", 10);
        final String json = objectMapper.writeValueAsString(testMap);

        assertThat(json).isNotEmpty();
        assertThat(json).isEqualTo("{\"intObject\":10}");
    }

    @Test
    public void shouldSerializeUsingLineProtocol() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("tag1Key", "tag1Value");
        tags.put("tag2Key", "tag2Value");
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("field1Key", "field1Value");
        fields.put("field2Key", "field2Value");
        InfluxDbPoint point1 = new InfluxDbPoint("measurement1", tags, "123", fields);
        Set<InfluxDbPoint> set = new HashSet<InfluxDbPoint>();
        set.add(point1);
        InfluxDbWriteObject influxDbWriteObject = mock(InfluxDbWriteObject.class);
        when(influxDbWriteObject.getPoints()).thenReturn(set);
        InfluxDbWriteObjectSerializer influxDbWriteObjectSerializer = new InfluxDbWriteObjectSerializer();
        String lineString = influxDbWriteObjectSerializer.getLineProtocolString(influxDbWriteObject);
        assertThat(lineString).isEqualTo(
            "measurement1,tag1Key=tag1Value,tag2Key=tag2Value field1Key=\"field1Value\",field2Key=\"field2Value\" 123000000\n");
        InfluxDbPoint point2 = new InfluxDbPoint("measurement1", tags, "123", fields);
        set.add(point2);
        lineString = influxDbWriteObjectSerializer.getLineProtocolString(influxDbWriteObject);
        assertThat(lineString).isEqualTo(
            "measurement1,tag1Key=tag1Value,tag2Key=tag2Value field1Key=\"field1Value\",field2Key=\"field2Value\" 123000000\n"
                + "measurement1,tag1Key=tag1Value,tag2Key=tag2Value field1Key=\"field1Value\",field2Key=\"field2Value\" 123000000\n");
    }
}
