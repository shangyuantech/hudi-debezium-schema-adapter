package org.apache.hudi.debezium.config;

import java.util.Map;

public class ZookeeperConfig {

    private String namespace = "hudi";

    private String service = "debezium";

    private String zkConnPath = "localhost:2181";

    private Integer masterLoopInterval = 2;

    private Integer zkClientSessionTimeout = 5000;

    private Integer zkClientConnectionTimeout = 5000;

    private Integer zkClientBaseSleepTimeMs = 1000;

    private Integer zkClientMaxRetries = 3;

    private Boolean curatorSingle = true;

    private Integer curatorCorePoolSize = 1;

    private Integer curatorMaxPoolSize = 10;

    private Integer curatorKeepAliveSeconds = 1000;

    private Integer curatorQueuingCapacity = 10;

    public ZookeeperConfig() {
        Map<String,String> envs = System.getenv();
        envs.forEach((k, v) -> {
            if (sameEnv(k, "HUDI_DBZ_NAMESPACE")) {
                setNamespace(v);
            } else if (sameEnv(k, "HUDI_DBZ_SERVICE")) {
                setService(v);
            } else if (sameEnv(k, "HUDI_DBZ_ZK_HOST")) {
                setZkConnPath(v);
            } else if (sameEnv(k, "HUDI_DBZ_ZK_LOOP_INTERVAL")) {
                setMasterLoopInterval(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_ZK_SESSION_TIMEOUT")) {
                setZkClientSessionTimeout(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_ZK_CONN_TIMEOUT")) {
                setZkClientConnectionTimeout(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_ZK_SLEEP_TIMEOUT")) {
                setZkClientBaseSleepTimeMs(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_ZK_MAX_RETRIES")) {
                setZkClientMaxRetries(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_CURATOR_SINGLE")) {
                setCuratorSingle(Boolean.valueOf(v));
            } else if (sameEnv(k, "HUDI_DBZ_CURATOR_CORE_POOL_SIZE")) {
                setCuratorCorePoolSize(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_CURATOR_MAX_POLL_SIZE")) {
                setCuratorMaxPoolSize(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_CURATOR_KEEP_ALIVE_SEC")) {
                setCuratorKeepAliveSeconds(Integer.parseInt(v));
            } else if (sameEnv(k, "HUDI_DBZ_CURATOR_QUEUE_CAPACITY")) {
                setCuratorQueuingCapacity(Integer.parseInt(v));
            }
        });
    }

    private boolean sameEnv(String envName, String propertyName) {
        if (envName.toUpperCase().equals(propertyName)) {
            return true;
        }
        return false;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getZkConnPath() {
        return zkConnPath;
    }

    public void setZkConnPath(String zkConnPath) {
        this.zkConnPath = zkConnPath;
    }

    public Integer getMasterLoopInterval() {
        return masterLoopInterval;
    }

    public void setMasterLoopInterval(Integer masterLoopInterval) {
        this.masterLoopInterval = masterLoopInterval;
    }

    public Integer getZkClientSessionTimeout() {
        return zkClientSessionTimeout;
    }

    public void setZkClientSessionTimeout(Integer zkClientSessionTimeout) {
        this.zkClientSessionTimeout = zkClientSessionTimeout;
    }

    public Integer getZkClientConnectionTimeout() {
        return zkClientConnectionTimeout;
    }

    public void setZkClientConnectionTimeout(Integer zkClientConnectionTimeout) {
        this.zkClientConnectionTimeout = zkClientConnectionTimeout;
    }

    public Integer getZkClientBaseSleepTimeMs() {
        return zkClientBaseSleepTimeMs;
    }

    public void setZkClientBaseSleepTimeMs(Integer zkClientBaseSleepTimeMs) {
        this.zkClientBaseSleepTimeMs = zkClientBaseSleepTimeMs;
    }

    public Integer getZkClientMaxRetries() {
        return zkClientMaxRetries;
    }

    public void setZkClientMaxRetries(Integer zkClientMaxRetries) {
        this.zkClientMaxRetries = zkClientMaxRetries;
    }

    public Boolean getCuratorSingle() {
        return curatorSingle;
    }

    public void setCuratorSingle(Boolean curatorSingle) {
        this.curatorSingle = curatorSingle;
    }

    public Integer getCuratorCorePoolSize() {
        return curatorCorePoolSize;
    }

    public void setCuratorCorePoolSize(Integer curatorCorePoolSize) {
        this.curatorCorePoolSize = curatorCorePoolSize;
    }

    public Integer getCuratorMaxPoolSize() {
        return curatorMaxPoolSize;
    }

    public void setCuratorMaxPoolSize(Integer curatorMaxPoolSize) {
        this.curatorMaxPoolSize = curatorMaxPoolSize;
    }

    public Integer getCuratorKeepAliveSeconds() {
        return curatorKeepAliveSeconds;
    }

    public void setCuratorKeepAliveSeconds(Integer curatorKeepAliveSeconds) {
        this.curatorKeepAliveSeconds = curatorKeepAliveSeconds;
    }

    public Integer getCuratorQueuingCapacity() {
        return curatorQueuingCapacity;
    }

    public void setCuratorQueuingCapacity(Integer curatorQueuingCapacity) {
        this.curatorQueuingCapacity = curatorQueuingCapacity;
    }
}
