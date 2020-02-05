package com.tusker.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
//        System.out.println("Hello World!!!");

        String bootstrapServers = "127.0.0.1:9092";

        // 1. create producer properties
        Properties properties = new Properties();
//        properties.setProperty( "bootstrap.servers", bootstrapServers );
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
//        properties.setProperty( "key.serializer", StringSerializer.class.getName() );
        properties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );
//        properties.setProperty( "value.serializer", StringSerializer.class.getName() );
        properties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );



        // 2. create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // 2.1 Producer Record
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<String,String>("first_topic","Hello World!!!");

        // 3. send data ( This is async. )
        producer.send( producerRecord );

        // flush data
        producer.flush();
        //flush code
        producer.close();
    }
}
