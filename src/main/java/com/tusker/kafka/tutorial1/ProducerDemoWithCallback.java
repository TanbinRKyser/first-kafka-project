package com.tusker.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
//        System.out.println("Hello World!!!");

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class );

        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        // 1. create producer properties
        Properties properties = new Properties();
//        properties.setProperty( "bootstrap.servers", bootstrapServers );
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER );
//        properties.setProperty( "key.serializer", StringSerializer.class.getName() );
        properties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );
//        properties.setProperty( "value.serializer", StringSerializer.class.getName() );
        properties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );



        // 2. create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // 2.1 Producer Record
            ProducerRecord<String,String> producerRecord =
                    new ProducerRecord<String,String>("first_topic","Hello World! " + i );

            // 3. send data ( This is async. )
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record is successfully sent or an exception is thrown
                    if( e == null ){
                        // The record was successfully sent.
                        logger.info( "Received new metadata. \n"
                                + "Topic: "+ recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "Offset: " + recordMetadata.offset() +"\n"
                                + "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error( "Error while producing. " + e );
                    }
                }
            });
        }

        // flush data
        producer.flush();
        //flush code
        producer.close();
    }
}
