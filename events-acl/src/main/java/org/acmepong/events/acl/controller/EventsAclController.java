package org.acmepong.events.acl.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.acmepong.events.acl.config.KafkaConsumerConfig;
import org.acmepong.events.acl.config.KafkaProducerConfig;
import org.acmepong.events.acl.model.EventsMessage;
import org.acmepong.events.acl.model.ResponseEntity;

@RestController
public class EventsAclController {
	
	private Consumer<String, String> consumer;
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@GetMapping(value="/events/test")	
	public String testPost() {
		EventsMessage eventMsg = new EventsMessage();
		eventMsg.setMessage("Hello World");
		logger.debug("eventmessage:" + eventMsg.getMessage());
		return eventMsg.getMessage();
	}
	
	@SuppressWarnings("unchecked")
	@GetMapping(value="/events/{topic}")	
	public Map<String, String> getMessages(@PathVariable String topic) {
			
		@SuppressWarnings("resource")
		ApplicationContext context = new AnnotationConfigApplicationContext(KafkaConsumerConfig.class);
		this.consumer = (Consumer<String, String>)context.getBean("kafkaConsumerBean");
			
		List<String> topics = new ArrayList<String>();
		topics.add(topic);						
		consumer.subscribe(topics);
	
		Map<String, String> recordMap = new HashMap<String, String>();
		
		logger.debug("===========Consumer=====================");
		logger.debug("Partition:"+consumer.partitionsFor(topic)+"topic:"+topic);		
		logger.debug("=======================================");
		
		ConsumerRecords<String, String>records = consumer.poll(100);
		logger.debug("No of records: " + records.count());
		
		logger.debug("===========Consumer=====================");
		for (ConsumerRecord<String, String> record : records) {
			recordMap.put(String.valueOf(record.offset()), record.value());
			logger.debug(record.partition()+":"+record.offset()+":"+record.value());			
		}
		logger.debug("===========--------=====================");
		
		return recordMap;		
	}
	
	
	@SuppressWarnings("unchecked")
	@PostMapping(value="/events/{topic}/send")	
	public ResponseEntity postMessage(@PathVariable String topic, @RequestBody EventsMessage eventMsg) {
		ResponseEntity responseEntity = new ResponseEntity(); 
		
		try {
			//get application context
			responseEntity.setMessage("success");
			
			@SuppressWarnings("resource")
			ApplicationContext context = new AnnotationConfigApplicationContext(KafkaProducerConfig.class);
						
			this.kafkaTemplate = (KafkaTemplate<String, String>)context.getBean("kafkaTemplateBean");
			logger.debug("===========Producer=====================");
			logger.debug("EventsMessage: " + eventMsg.getMessage());
			logger.debug("===========Producer=====================");
								
			kafkaTemplate.send(topic, eventMsg.getMessage());

		} catch (Exception e) {
			responseEntity.setMessage(e.getMessage());
		}
		
		return responseEntity;
		
	}
}
