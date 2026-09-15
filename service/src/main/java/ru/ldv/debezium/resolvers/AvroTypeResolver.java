package ru.ldv.debezium.resolvers;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class AvroTypeResolver {

    /**
     * Конвертирует значение из GenericRecord на основе метаданных Avro Schema.
     */
    public Object resolveValue(Schema fieldSchema, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        Schema actualSchema = unwrapUnion(fieldSchema);

        String connectName = actualSchema.getProp("connect.name");
        org.apache.avro.LogicalType logicalType = actualSchema.getLogicalType();

        if ("io.debezium.time.Date".equals(connectName) || logicalType instanceof LogicalTypes.Date) {
            long days = ((Number) rawValue).longValue();
            return LocalDate.ofEpochDay(days);
        }

        if ("io.debezium.time.MicroTimestamp".equals(connectName)) {
            long micros = ((Number) rawValue).longValue();
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(micros / 1000), ZoneOffset.UTC);
        }

        if ("io.debezium.time.Timestamp".equals(connectName) || logicalType instanceof LogicalTypes.TimestampMillis) {
            long millis = ((Number) rawValue).longValue();
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        }

        if ("io.debezium.time.ZonedTimestamp".equals(connectName)) {
            return java.time.OffsetDateTime.parse(rawValue.toString());
        }

        if (logicalType instanceof LogicalTypes.Decimal decimalType) {
            if (rawValue instanceof ByteBuffer byteBuffer) {
                byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.duplicate().get(bytes);
                return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
            } else if (rawValue instanceof byte[] bytes) {
                return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
            }
        }

        if ("io.debezium.data.Uuid".equals(connectName)) {
            return java.util.UUID.fromString(rawValue.toString());
        }

        if (rawValue instanceof org.apache.avro.util.Utf8) {
            return rawValue.toString();
        }

        return rawValue;
    }

    private Schema unwrapUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }
}
