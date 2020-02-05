package com.tusker.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        System.out.println("Hello World!!!");

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class );

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
            final String TOPIC = "first_topic";
            String value = "Hello World " + Integer.toString( i );
            String key = "Id_" + Integer.toString( i );

            ProducerRecord<String,String> producerRecord =
                    new ProducerRecord<String,String>( TOPIC, key, value );

            logger.info( "Key " + key );
            // id_0 is going to partition 2
            // id_1 is going to partition 1
            // id_2 is going to partition 2
            // id_3 is going to partition 0
            // id_4 is going to partition 1
            // id_5 is going to partition 2
            // id_6 is going to partition 0
            // id_7 is going to partition 2
            // id_8 is going to partition 1
            // id_9 is going to partition 0

            // 3. send data ( This is asynchronous )
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
            }).get(); // block the .send() to make it synchronous - don't do it in prod.
        }

        // flush data
        producer.flush();
        //flush code
        producer.close();
    }
}
