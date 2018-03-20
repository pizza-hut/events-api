package org.acmepong.events.acl.config;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Configuration
@Component
@PropertySource("classpath:application.properties")
public class KafkaConsumerConfig {
      
	@Autowired
	Environment env;
	
	@Value("${kafka.bootstrapAddress}")
	private String kafka_bootstrapAddress;
	//private String kafka_bootstrapAddress="velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
	
	@Value("${kafka.username}")
    private String kafka_username;
	//private String kafka_username="bpgxuf51";
	
    @Value("${kafka.password}")
    private String kafka_password;
    //private String kafka_password="Lf8Jljo3W7gA5lLEEjot-oHGZOWIddkP";
    
    @Value("${spring.kafka.client-id}")
    private String spring_kafka_client_id;
    
    @Value("${spring.kafka.client-id}")
    private String spring_kafka_consumer_group_id;
    
	public Properties consumerConfigs() {
    	String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    	
    	
    	//String usr="bpgxuf51";
    	//String pwd="Lf8Jljo3W7gA5lLEEjot-oHGZOWIddkP";    	
    	String jaasCfg = String.format(jaasTemplate, this.kafka_username, this.kafka_password);
        
        Properties props = new Properties();
        //Map<String, Object> configProps = new HashMap<>();        
        //String bootstrapAddress="velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
        System.out.println("===========================Consumer===========================================");
        System.out.println("Consumer Bootstrap Address: " + this.kafka_bootstrapAddress);
        System.out.println("==============================================================================");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, this.spring_kafka_client_id);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka_bootstrapAddress);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");        
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_SSL");        
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.spring_kafka_consumer_group_id);
        
        System.out.println("========================Consumer==============================================");
        System.out.println("Consumer security: " + jaasCfg);
        System.out.println("==============================================================================");
        return props;		
	}
    
    KafkaConsumerFactory kafkaConsumerFactory() {
    	return new KafkaConsumerFactory();
    }
        
    @Bean(name="kafkaConsumerBean")
    public KafkaConsumer<String, String> getConsumer(Properties configProps) throws Exception {
    	return kafkaConsumerFactory().getKafkaConsumer(consumerConfigs());
    }

}
