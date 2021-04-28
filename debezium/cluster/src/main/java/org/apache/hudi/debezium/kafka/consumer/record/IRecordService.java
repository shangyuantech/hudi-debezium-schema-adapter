package org.apache.hudi.debezium.kafka.consumer.record;

import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

public interface IRecordService {

    Optional<SchemaRecord> listen(ConsumerRecord<?, ?> record) throws Exception;

    Optional<Task<?, ?>> publishTask(SchemaRecord record) throws Exception ;
}
