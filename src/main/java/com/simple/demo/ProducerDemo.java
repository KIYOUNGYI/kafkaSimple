package com.simple.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 데모 앱
 */
public class ProducerDemo 
{
	static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main( String[] args )
    {
    	Properties properties = new Properties();
    	
    	// producer properties 생성
    	String bootstrapServer = "127.0.0.1:9092";

    	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
   
//    	properties.setProperty("bootstrap.servers", bootstrapServer);
//    	properties.setProperty("key.serializer", StringSerializer.class.getName());	
//    	properties.setProperty("value.serializer", StringSerializer.class.getName());
    	
    	// producer 생성
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    	
    	// 전송 async(비동기)
//    	producer.send(record);
    	for(int i=0;i<20;i++) {
    		
    		String topic = "first_topic";
    		String key = "_id "+Integer.toString(i);
    		String value = "way to go~"+Integer.toString(i);
    		
    		logger.info("Key : "+key);
    		
    		// producer record 생성
        	ProducerRecord< String, String> record = new ProducerRecord<String, String>(topic,key,value);
	    	producer.send(record,new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					// executes everytime a record is successfully sent or exception is thrown 
					if(e==null) 
					{
						//레코드 전송 성공
						logger.info("Received new meta data\n"+" topic:"+metadata.topic()
						+" partition:"+metadata.partition()
						+" offset:"+metadata.offset()
						+" timestamp:"+metadata.timestamp());
					}
					else 
					{
						logger.error("Error occured : ",e);
					}
				}
			});//.get() <- 동기 방식 (실제 production에서 이거 사용하면 큰일날듯) 왜 있는지 잘 모르겠다.
    	
    	}
    	// buffered records immediately available to send 
    	producer.flush();
    	producer.close();
    }
}
