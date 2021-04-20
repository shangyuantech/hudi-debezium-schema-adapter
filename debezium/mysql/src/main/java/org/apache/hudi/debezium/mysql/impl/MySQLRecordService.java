package org.apache.hudi.debezium.mysql.impl;

import org.apache.hudi.debezium.kafka.consumer.IRecordService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLRecordService implements IRecordService {

    private final static Logger logger = LoggerFactory.getLogger(MySQLRecordService.class);

    @Override
    public void listen(ConsumerRecord<?, ?> record) {
        if (logger.isDebugEnabled()) {
            logger.debug("receive consumer record {}", record);
        }
    }
}
