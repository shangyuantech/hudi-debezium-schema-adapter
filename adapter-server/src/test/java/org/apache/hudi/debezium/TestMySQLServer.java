package org.apache.hudi.debezium;

import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

public class TestMySQLServer {

    private TestingServer testingServer;

    private EmbeddedKafkaCluster kafka;

    DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder()
            .setPort(3307)
            .setDataDir("/tmp/mysql/test_mysql_server");

    @Rule
    public MariaDB4jRule dbRule = new MariaDB4jRule(
            configBuilder.build(),
            "test_database",
            "sql/test_table.sql"
    );

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < Objects.requireNonNull(children).length; i++) {
                deleteDir(new File(dir, children[i]));
            }
        }
        dir.delete();
    }

    @Before
    public void before() throws Exception {
        // start zookeeper
        File tmp = new File("/tmp/zookeeper/test_server");
        if (tmp.exists()) {
            deleteDir(tmp);
        }
        tmp.mkdirs();
        testingServer = new TestingServer(2181, tmp);
        testingServer.start();

        // start kafka
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();
    }

    @Test
    public void testServer() throws InterruptedException {
        AdapterServer.start();
        //AdapterServer.start(false);
        //Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    }
}
