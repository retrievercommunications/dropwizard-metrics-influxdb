package com.izettle.metrics.influxdb.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.izettle.metrics.influxdb.data.InfluxDbPoint;
import com.izettle.metrics.influxdb.data.InfluxDbWriteObject;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDbWriteObjectSerializer {

    private static final Escaper FIELD_ESCAPER = Escapers.builder().addEscape('"', "\\\"").build();
    private static final Escaper KEY_ESCAPER = Escapers.builder()
        .addEscape(' ', "\\ ")
        .addEscape(',', "\\,")
        .addEscape('=', "\\=")
        .build();

    protected static class MapSerializer<P, Q> extends JsonSerializer<Map<P, Q>> {
        @Override
        public void serialize(
            final Map<P, Q> influxDbMap,
            final JsonGenerator jsonGenerator,
            final SerializerProvider provider)
            throws IOException {
            if (influxDbMap != null) {
                jsonGenerator.writeStartObject();
                for (Map.Entry<P, Q> entry : influxDbMap.entrySet()) {
                    jsonGenerator.writeFieldName(entry.getKey().toString());
                    jsonGenerator.writeObject(entry.getValue());
                }
                jsonGenerator.writeEndObject();
            }
        }
    }

    private final ObjectMapper objectMapper;

    public InfluxDbWriteObjectSerializer() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        final SimpleModule module = new SimpleModule("SimpleModule", new Version(1, 0, 0, null, null, null));
        module.addSerializer(Map.class, new MapSerializer());
        objectMapper.registerModule(module);
    }

    @Deprecated
    public String getJsonString(InfluxDbWriteObject influxDbWriteObject) throws Exception {
        return objectMapper.writeValueAsString(influxDbWriteObject);
    }
    // measurement[,tag=value,tag2=value2...] field=value[,field2=value2...] [unixnano]

    /**
     * calculate the lineprotocol for all Points.
     *
     * @return the String with newLines.
     */
    public String getLineProtocolString(InfluxDbWriteObject influxDbWriteObject) {
        StringBuilder sb = new StringBuilder();
        for (InfluxDbPoint point : influxDbWriteObject.getPoints()) {
            sb.append(lineProtocol(point)).append("\n");
        }
        return sb.toString();
    }

    public String lineProtocol(InfluxDbPoint point) {
        final StringBuilder sb = new StringBuilder();
        sb.append(KEY_ESCAPER.escape(point.getMeasurement()));
        sb.append(concatenatedTags(point.getTags()));
        sb.append(concatenateFields(point.getFields()));
        sb.append(formatedTime(Long.valueOf(point.getTimestamp())));
        return sb.toString();
    }

    private StringBuilder concatenatedTags(Map<String, String> tags) {
        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            sb.append(",");
            sb.append(KEY_ESCAPER.escape(tag.getKey())).append("=").append(KEY_ESCAPER.escape(tag.getValue()));
        }
        sb.append(" ");
        return sb;
    }

    private StringBuilder concatenateFields(Map<String, Object> fields) {
        final StringBuilder sb = new StringBuilder();
        final int fieldCount = fields.size();
        int loops = 0;

        NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
        numberFormat.setMaximumFractionDigits(340);
        numberFormat.setGroupingUsed(false);
        numberFormat.setMinimumFractionDigits(1);

        for (Map.Entry<String, Object> field : fields.entrySet()) {
            sb.append(KEY_ESCAPER.escape(field.getKey())).append("=");
            loops++;
            Object value = field.getValue();
            if (value instanceof String) {
                String stringValue = (String) value;
                sb.append("\"").append(FIELD_ESCAPER.escape(stringValue)).append("\"");
            } else if (value instanceof Number) {
                sb.append(numberFormat.format(value));
            } else {
                sb.append(value);
            }

            if (loops < fieldCount) {
                sb.append(",");
            }
        }
        return sb;
    }

    private StringBuilder formatedTime(Long time) {
        final StringBuilder sb = new StringBuilder();
        if (null == time) {
            time = System.nanoTime();
        }
        sb.append(" ").append(TimeUnit.NANOSECONDS.convert(time, TimeUnit.MILLISECONDS));
        return sb;
    }
}
