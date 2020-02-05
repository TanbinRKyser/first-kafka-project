package com.tusker.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithAssignAndSeek {
    public static void main(String[] args) {
//        System.out.println("Hello World!!!");
        Logger logger = LoggerFactory.getLogger( ConsumerDemoWithAssignAndSeek.class );

        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        final String groupId = "my-seven-app";
        final String topic = "first_topic";

        // Create a consumer config
        Properties properties = new Properties();
        properties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER );
        properties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
        properties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );

        properties.setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );

        // Create a consumer
        KafkaConsumer< String, String > consumer = new KafkaConsumer<String, String>( properties );

        // assign and seek are mostly used to replay data or fetch a specific message.
        TopicPartition partitionToReadFrom = new TopicPartition( topic, 0 );
        long offsetToReadFrom = 15L;

        // assign
        consumer.assign( Arrays.asList( partitionToReadFrom ));

        // seek
        consumer.seek( partitionToReadFrom, offsetToReadFrom );


        int noOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while ( keepOnReading ){
            ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 100 ) );

            for ( ConsumerRecord<String, String> record : records ){
                numberOfMessagesReadSoFar += 1;
                logger.info( "Key: " + record.key() + ", Value: " + record.value() );
                logger.info( "Partition: " + record.partition() + ", Offset: " + record.offset() );

                if( numberOfMessagesReadSoFar >= noOfMessagesToRead ){
                    keepOnReading = false; // to exit the while loop
                    break; // to exit
                }
            }
        }

        logger.info("Exiting the application.");

    }
}
