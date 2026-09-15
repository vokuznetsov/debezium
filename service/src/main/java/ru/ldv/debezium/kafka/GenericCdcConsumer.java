package ru.ldv.debezium.kafka;

import ru.ldv.debezium.resolvers.AvroTypeResolver;
import ru.ldv.debezium.services.GenericSyncService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class GenericCdcConsumer {

    private final GenericSyncService syncService;
    private final AvroTypeResolver avroTypeResolver;
    private final AtomicLong processedCount = new AtomicLong(0);

    @KafkaListener(topics = "${cdc.topic:cdc.events}")
    public void consume(ConsumerRecord<GenericRecord, GenericRecord> consumerRecord, Acknowledgment ack) {
        GenericRecord record = consumerRecord.value();

        if (record == null) {
            ack.acknowledge();
            processedCount.incrementAndGet();
            return;
        }

        // Динамически извлекаем схему и таблицу из сообщения
        String schemaName = record.get("__schema").toString();
        String tableName = record.get("__table").toString();
        String operation = record.get("__op").toString();

        // Динамическое извлечение первичного ключа из kafka key
        List<String> primaryKeys = new ArrayList<>();
        GenericRecord keyRecord = consumerRecord.key();

        if (keyRecord != null && keyRecord.getSchema() != null) {
            primaryKeys = keyRecord.getSchema().getFields().stream()
                    .map(Schema.Field::name)
                    .filter(name -> !name.startsWith("__"))
                    .toList();
        }

        if (primaryKeys.isEmpty()) {
            primaryKeys = List.of("id");
        }

        Map<String, Object> rowMap = avroRecordToMap(record);

        if ("d".equalsIgnoreCase(operation)) {
            syncService.delete(schemaName, tableName, rowMap, primaryKeys);
        } else {
            syncService.upsert(schemaName, tableName, rowMap, primaryKeys);
        }

        long currentCount = processedCount.incrementAndGet();
        ack.acknowledge();

        log.info(
                "Обработано CDC-сообщение #{} [{}]. Таблица: {}.{}, Операция: {}",
                currentCount,
                consumerRecord.topic(),
                schemaName,
                tableName,
                operation
        );
    }

    private Map<String, Object> avroRecordToMap(GenericRecord record) {
        if (record == null) return Map.of();

        List<Schema.Field> fields = record.getSchema().getFields();

        // Сразу задаем capacity, чтобы HashMap ни разу не расширялся во время цикла (микрооптимизация)
        Map<String, Object> map = new HashMap<>((int) (fields.size() / 0.75f) + 1);

        for (Schema.Field field : fields) {
            String fieldName = field.name();

            // Игнорируем служебные поля Debezium
            if (fieldName.startsWith("__")) {
                continue;
            }

            Object rawValue = record.get(fieldName);
            Object convertedValue = avroTypeResolver.resolveValue(field.schema(), rawValue);
            map.put(fieldName, convertedValue);
        }

        return map;
    }

}
