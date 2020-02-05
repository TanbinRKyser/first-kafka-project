package com.tusker.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
//        System.out.println("Hello World!!!");
        Logger logger = LoggerFactory.getLogger( ConsumerDemoGroups.class );

        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        final String groupId = "my-fifth-app";
        final String topic = "first_topic";

        // Create a consumer config
        Properties properties = new Properties();
        properties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER );
        properties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
        properties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
        properties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, groupId );
        properties.setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );

        // Create a consumer
        KafkaConsumer< String, String > consumer = new KafkaConsumer<String, String>( properties );

        // Subscribe to our topic
//        consumer.subscribe( Collections.singleton( "first_topic" ) );
        consumer.subscribe( Arrays.asList( topic ) );

        // poll for new data
        while ( true ){
            ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 100 ) );

            for ( ConsumerRecord< String, String > record : records ){
                logger.info( "Key: " + record.key() + ", Value: " + record.value() );
                logger.info( "Partition: " + record.partition() + ", Offset: " + record.offset() );
            }
        }

    }
}
