package com.simple.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerRunnable implements Runnable {

	private CountDownLatch latch;
	private KafkaConsumer<String, String> consumer;
	static Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

	public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapServer, String groupId) {
		this.latch = latch;
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//create consumer
		consumer = new KafkaConsumer<String, String>(properties);
		//subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		
	}

	public void run() {
		// TODO Auto-generated method stub
		try {
			while (true) {
				ConsumerRecords<String, String> records =
						consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> rec : records) {
					logger.info("Key : " + rec.key() + " , value : " + rec.value());
					logger.info("partition : " + rec.partition() + " offset: " + rec.offset() + " ");
				}
			}
		} catch (WakeupException e) {
			logger.info("Received shutdown signal ");
			
		} finally {
			consumer.close();
			latch.countDown();//메인코드에 컨슈밍이 끝났음을 알리는 것
		}

	}

	public void shutdown() {
		// 인터럽 하는 것
		consumer.wakeup();
	}

}
