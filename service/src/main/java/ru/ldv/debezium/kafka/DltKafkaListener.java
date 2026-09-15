package ru.ldv.debezium.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DltKafkaListener {

    @KafkaListener(topics = "${cdc.dlt-topic:cdc.events-dlt}", groupId = "cdc-dlt-group")
    public void listenDlt(ConsumerRecord<GenericRecord, GenericRecord> record) {
        log.error(
                "CRITICAL: Received message in dlt! Key: {}, Partition: {}, Offset: {}",
                record.key(), record.partition(), record.offset()
        );
    }
}
