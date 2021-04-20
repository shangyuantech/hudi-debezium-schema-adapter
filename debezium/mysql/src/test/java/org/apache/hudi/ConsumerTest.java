//package org.apache.hudi;
//
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentLinkedQueue;
//
//public class ConsumerTest {
//
//    public static void main(String[] args) {
//        final ConcurrentLinkedQueue<String> subscribedTopics = new ConcurrentLinkedQueue<>();
//
//        // ������һ�������̣߳�������������ͣ10��Ȼ����topic����
//        Runnable runnable = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    // swallow it.
//                }
//                subscribedTopics.addAll(Arrays.asList("test2"));
//            }
//        };
//        new Thread(runnable).start();
//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.3.101.60:9092");
//        props.put("group.id", "my-group1");
//        props.put("auto.offset.reset", "earliest");
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//
//        consumer.subscribe(Arrays.asList("test1"));
//        while (true) {
//            consumer.poll(2000); //��ʾÿ2��consumer���л���ȥ��ѯһ�¶���״̬�Ƿ���Ҫ���
//            // ��������ע��Ϣ���ѣ����ÿ��ֻ�Ǵ�ӡ���Ľ����
//            System.out.println(consumer.subscription());
//            if (!subscribedTopics.isEmpty()) {
//                Iterator<String> iter = subscribedTopics.iterator();
//                List<String> topics = new ArrayList<>();
//                while (iter.hasNext()) {
//                    topics.add(iter.next());
//                }
//                subscribedTopics.clear();
//                consumer.subscribe(topics); // ���¶���topic
//            }
//        }
//        // ����ֻ�ǲ���֮�ã�ʹ����while(true)����������û����ʽ�ر�consumer
////        consumer.close();
//    }
//}
