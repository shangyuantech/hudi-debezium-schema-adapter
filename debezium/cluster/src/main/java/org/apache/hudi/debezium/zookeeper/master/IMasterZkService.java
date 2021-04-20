package org.apache.hudi.debezium.zookeeper.master;

public interface IMasterZkService {

    void isLeader() throws Exception;

    void notLeader() throws Exception;
}
