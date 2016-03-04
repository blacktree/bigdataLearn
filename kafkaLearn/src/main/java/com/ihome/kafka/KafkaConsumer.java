package com.ihome.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {

	private final ConsumerConnector consumer;

	public KafkaConsumer() {
		Properties props = new Properties();
		//zookeeper 配置
		props.put("zookeeper.connect", "10.166.224.119:2181");
		//group 代表一个消费组
		props.put("group.id", "jd-group");
		//zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		//序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ConsumerConfig config = new ConsumerConfig(props);

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

    void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //我们要告诉kafka该进程会有多少个线程来处理对应的topic
        int a_numThreads = 3;
     // 用3个线程来处理topic
        topicCountMap.put(KafkaProducer.TOPIC, a_numThreads);

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(KafkaProducer.TOPIC).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext())
        	
//        	System.out.println(it.next().key());
            System.out.println(it.next().message());
    }

    public static void main(String[] args) {
        new KafkaConsumer().consume();
    }
}
