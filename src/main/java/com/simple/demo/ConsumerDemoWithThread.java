package com.simple.demo;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {
	static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
		
	}
	
	private void run() 
	{
		String groupId = "my-fourth-app";
		String topic = "first_topic";
		// producer properties 생성
		String bootstrapServer = "127.0.0.1:9092";
		CountDownLatch latch = new CountDownLatch(1);

		logger.info("Creating the Consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(latch,topic,bootstrapServer,groupId);
		
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			
			((ConsumerRunnable)myConsumerRunnable).shutdown();
			try {
				logger.info("caught shutdown");
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("app exit");
		}
		));
		
		try 
		{
			latch.await();
		}
		catch(InterruptedException e) 
		{
			logger.info("App got interrupted ",e);
		}finally 
		{
			logger.info("closing app");
		}
	}
	
	private ConsumerDemoWithThread()
	{
		
	}
	
	
}
