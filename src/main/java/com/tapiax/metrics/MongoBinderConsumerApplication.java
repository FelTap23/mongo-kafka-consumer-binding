package com.tapiax.metrics;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

@SpringBootApplication
@EnableBinding(Sink.class)
public class MongoBinderConsumerApplication {

	private Logger logger  = LoggerFactory.getLogger(MongoBinderConsumerApplication.class);
	public static void main(String[] args) {
		SpringApplication.run(MongoBinderConsumerApplication.class, args);
	}
	
	
	@StreamListener(Sink.INPUT)
	public void processMesssage(Message<Document> message) {
		Acknowledgment acknowledgment  = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
		if(acknowledgment != null) {
			System.out.println(message.toString());
			logger.info("Acnowledment provided");
			acknowledgment.acknowledge();
		}
	}

}
