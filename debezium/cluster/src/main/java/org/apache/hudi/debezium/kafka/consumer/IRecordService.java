package org.apache.hudi.debezium.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IRecordService {

    void listen(ConsumerRecord<?, ?> record);
}
