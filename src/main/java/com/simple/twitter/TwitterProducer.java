package com.simple.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * reference: https://github.com/twitter/hbc
 * 
 * @author yky1990
 *
 */
public class TwitterProducer {
	static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	String consumerKey = "EzpTu2UJIoSv0dHpHnjGSMLt2";
	String consumerSecret = "HaRZFcnfnW7BAvyEHIuE3Nn4iLEXwaZVLLwpxLJmTk9CcRiMLK";
	String token = "1181487034386378752-sgjWDzL5o1Wc7Ha1MCHkDbl0qvCor0";
	String secret = "7iPjNmT2hqAODkHGxsZM8Ho9QhMznIByiHNEUPVGRcaIg";
	List<String> terms = Lists.newArrayList("Manchester United","bitcoin","usa","sport","soccer");

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// Create a twitter client
		Client client = createTwitterClient(msgQueue);

		client.connect();
		// create a twitter producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// loop to send tweets to Kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("Timeout Exception : ", e);
				client.stop();
			}
			if (msg != null) {
				logger.info("msg : " + msg);
				producer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						// TODO Auto-generated method stub
						if (e != null) {
							logger.error("something bad happened", e);
						}
					}
				});
			}
		}
		logger.info("End of app");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

//		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms

//		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Ki's machine") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
//				  .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
//				hosebirdClient.connect();
		return hosebirdClient;
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServer = "127.0.0.1:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create a safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");//중복 방지를 위한 설정
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //KAFKA 2.0 >=1.0   2.0 이상의 버전에서는 5로 설정 . 그 이외 버전은 1 
		
		// high throughput producer (latency & cpu usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");// 구글에서 만든 오픈소스 
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");// 메시지 압축 -> 디스크,cpu,네트워크 절약
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));//32KB BATCH SIZE 
		
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
		return kafkaProducer;
	}
}
//Lingering allows to wait a bit of time before sending messages and increases chances of batching
//when I compress my messages from the producer side, the consumers have to decompress them. (only consumers de-compress the messages!)
//http://happinessoncode.com/2019/01/18/kafka-compression-ratio/
//https://team-platform.tistory.com/32