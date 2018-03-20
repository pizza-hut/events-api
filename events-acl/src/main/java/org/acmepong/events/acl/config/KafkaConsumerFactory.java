package org.acmepong.events.acl.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.config.AbstractFactoryBean;

public class KafkaConsumerFactory extends AbstractFactoryBean<KafkaConsumer<String, String>> {
	private static KafkaConsumer<String, String> kafkaConsumer = null;
	//private String consumerInstanceId;
	
	@Override
	public Class<?> getObjectType() {		
		return KafkaConsumer.class;
	}

	@Override
	protected KafkaConsumer<String, String> createInstance() throws Exception {
		// TODO Auto-generated method stub
		return KafkaConsumerFactory.kafkaConsumer;
	}
	
	
	public KafkaConsumer<String, String> getKafkaConsumer(Properties props) {		
		if (KafkaConsumerFactory.kafkaConsumer == null) {
			KafkaConsumerFactory.kafkaConsumer = new KafkaConsumer<String, String>(props);
		}		
		return KafkaConsumerFactory.kafkaConsumer;
	}

	
}
