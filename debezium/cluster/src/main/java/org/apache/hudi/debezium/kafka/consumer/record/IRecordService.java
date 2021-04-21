package org.apache.hudi.debezium.kafka.consumer.record;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IRecordService {

    SchemaRecord listen(ConsumerRecord<?, ?> record) throws Exception;

    void publishTask(SchemaRecord record) throws Exception ;
}
