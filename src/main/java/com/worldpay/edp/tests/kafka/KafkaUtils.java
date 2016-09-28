package com.worldpay.edp.tests.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaUtils {
	
	
	public static void consumerMsgs(String url, String topic, int partition, int MaxMsg){
		// init 
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "tests_kafka");
		props.put("security.protocol", "SASL_SSL");
		//
		try {
			Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
			//
			TopicPartition tp = new TopicPartition(topic, partition);
			consumer.assign(Arrays.asList(tp));
			//
			consumer.seekToBeginning(tp);
			boolean running = true;
			int max = MaxMsg;
			int curr=0;
			while(running){
				ConsumerRecords<String, String> crs = consumer.poll(1000);
				curr+=crs.count();
				if(curr>max){
					running=false;
				}
				for(ConsumerRecord<String, String> cr:crs){
					System.out.println(cr.offset()+" : "+cr.value());
				}
				
			}
			consumer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
