package com.ihome.kafka;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	private final Producer<String,String> producer;
	public final static String TOPIC= "spantest";
	
	public final static String span="{test}";
 
	public KafkaProducer() {
		Properties props = new Properties();
		//此处配置的是kafka的端口
        props.put("metadata.broker.list", "10.166.224.119:9092");
        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
	}
	
	void produce(){
        producer.send(new KeyedMessage<String, String>(TOPIC,null, span));
	}

	public static void main(String[] args) {
			new KafkaProducer().produce();
	}

}
