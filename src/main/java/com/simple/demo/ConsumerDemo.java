package com.simple.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	
	static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
	public static void main(String[] args) {
    	
		String groupId = "my-fourth-app";
		String topic = "first_topic";
    	// producer properties 생성
    	String bootstrapServer = "127.0.0.1:9092";
    	Properties properties = new Properties();
    	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    	properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    	
    	consumer.subscribe(Arrays.asList(topic));
    	
    	while(true) 
    	{
    		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    		
    		for(ConsumerRecord<String, String> rec : records) 
    		{
    			logger.info("Key : "+rec.key()+" , value : "+rec.value());
    			logger.info("partition : "+rec.partition()+" offset: "+rec.offset()+" ");
    
    			
    		}
    	}
    	
	}
}
