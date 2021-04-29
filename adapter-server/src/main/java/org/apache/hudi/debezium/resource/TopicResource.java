package org.apache.hudi.debezium.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("/topic")
public class TopicResource {

    private final static Logger logger = LoggerFactory.getLogger(TopicResource.class);

    private final ZookeeperConnector zkConnector;

    public TopicResource(ZookeeperConnector zkConnector) {
        this.zkConnector = zkConnector;
    }

    private List<String> getTopicList() throws Exception {
        String topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());
        List<String> topicList;
        if (zkConnector.dataExists(topicPath) == null) {
            topicList = Collections.emptyList();
        } else {
            topicList = zkConnector.getChildes(topicPath);
        }
        return topicList;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String topicList() throws Exception {
        return JsonUtils.writeValueAsString(getTopicList());
    }

    @GET
    @Path("/leader")
    @Produces(MediaType.TEXT_PLAIN)
    public String leader() throws Exception {
        String leaderPath = ZooKeeperUtils.getLeaderPath(zkConnector.getConfig().getService());
        if (zkConnector.dataExists(leaderPath) == null) {
            return "";
        } else {
            return zkConnector.getData(leaderPath);
        }
    }

    @GET
    @Path("/slave")
    @Produces(MediaType.APPLICATION_JSON)
    public String slave() throws Exception {
        String slavePath = ZooKeeperUtils.getSlaveBasePath(zkConnector.getConfig().getService());
        List<String> slaveList = zkConnector.getChildes(slavePath);
        return JsonUtils.writeValueAsString(slaveList);
    }

    @GET
    @Path("/task/{topic}")
    @Produces(MediaType.APPLICATION_JSON)
    public String topicTaskList(@PathParam("topic") String topic) throws Exception {
        String topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());
        List<String> topicList = new ArrayList<>();

        String taskPath = String.format("%s/%s", topicPath, topic);
        if (zkConnector.dataExists(taskPath) != null) {
            topicList.addAll(zkConnector.getChildes(taskPath));
        }

        return JsonUtils.writeValueAsString(topicList);
    }

    @GET
    @Path("/task")
    @Produces(MediaType.APPLICATION_JSON)
    public String totalTaskList() throws Exception {
        String topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());
        List<String> topicList = getTopicList();
        LinkedHashMap<String, List<String>> tasks = new LinkedHashMap<>();

        for (String topic : topicList) {
            String taskPath = String.format("%s/%s", topicPath, topic);
            if (zkConnector.dataExists(taskPath) != null) {
                List<String> subTaskList = zkConnector.getChildes(taskPath);
                tasks.put(topic, subTaskList);
            }
        }

        return JsonUtils.writeValueAsString(tasks);
    }

    @GET
    @Path("/config/{topic}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getTopicConfig(@PathParam("topic") String topic) {
        String topicPath = String.format("%s/%s", ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService()), topic);
        Optional<String> data = getZkNodeData(topicPath);
        return data.orElse("{}");
    }

    @GET
    @Path("/offset/{topic}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getKafkaOffset(@PathParam("topic") String topic) throws JsonProcessingException {
        String kafkaPath = String.format("%s/%s", ZooKeeperUtils.getKafkaPath(zkConnector.getConfig().getService()), topic);
        Optional<String> data = getZkNodeData(kafkaPath);
        LinkedHashMap<String, String> offset = new LinkedHashMap<>();
        if (data.isPresent()) {
            String[] splits = data.get().split(",");
            for (int i = 0; i < splits.length; i++) {
                String[] subSplits = splits[i].split(":");
                offset.put(subSplits[0], subSplits[1]);
            }
        } else {
            return "[]";
        }
        return JsonUtils.writeValueAsString(offset);
    }

    private Optional<String> getZkNodeData(String path) {
        String data = null;
        try {
            data = zkConnector.getData(path);
        } catch (Exception e) {
            logger.error("Error when get data from path " + path, e);
        }
        return Optional.ofNullable(data);
    }
}
