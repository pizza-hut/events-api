package org.acmepong.events.acl.service;

import java.util.List;

import org.acmepong.events.acl.config.KafkaConsumerConfig;
import org.acmepong.events.acl.config.KafkaProducerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventsAclService {
	private Consumer<String, String> consumer;
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@SuppressWarnings("unchecked")
	public void publish(String topic, String data) {
		@SuppressWarnings("resource")
		ApplicationContext context = new AnnotationConfigApplicationContext(KafkaProducerConfig.class);					
		this.kafkaTemplate = (KafkaTemplate<String, String>)context.getBean("kafkaTemplateBean");
		logger.debug(topic+""+data);
		kafkaTemplate.send(topic, data);		
	}
	
	@SuppressWarnings("unchecked")
	public ConsumerRecords<String, String> consume(List<String> topics, long timeout) {
		ConsumerRecords<String, String> consumerRecords=null;		
		@SuppressWarnings("resource")
		ApplicationContext context = new AnnotationConfigApplicationContext(KafkaConsumerConfig.class);
		this.consumer = (Consumer<String, String>)context.getBean("kafkaConsumerBean");
		consumer.subscribe(topics);
		logger.debug(topics.toString());
		consumerRecords = consumer.poll(timeout);
		logger.debug(String.valueOf(consumerRecords.count()));
		return consumerRecords;
	}
}
