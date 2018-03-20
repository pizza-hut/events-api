package org.acmepong.events.acl.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
//import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Component
@PropertySource("classpath:application.properties")
public class KafkaProducerConfig {
	
    @Value("${kafka.bootstrapAddress}")
    private String kafka_bootstrapAddress;
    
    @Value("${kafka.username}")
    private String kafka_usr;
    
    @Value("${kafka.password}")
    private String kafka_pwd;
	
	@Bean(name="kafkaTemplateBean")
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(ProducerFactory());
	}
    
    @Bean
	public ProducerFactory<String, String> ProducerFactory() {
    	String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    	//usr="bpgxuf51";
    	//pwd="Lf8Jljo3W7gA5lLEEjot-oHGZOWIddkP";
        String jaasCfg = String.format(jaasTemplate, this.kafka_usr, this.kafka_pwd);
        
        Map<String, Object> configProps = new HashMap<>();
        //bootstrapAddress="velomobile-01.srvs.cloudkafka.com:9094";
        //configProps.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer-1");
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka_bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put("security.protocol", "SASL_SSL");        
        configProps.put("sasl.mechanism", "SCRAM-SHA-256");
        configProps.put("sasl.jaas.config", jaasCfg);        
        return new DefaultKafkaProducerFactory<>(configProps);		
	}

}
