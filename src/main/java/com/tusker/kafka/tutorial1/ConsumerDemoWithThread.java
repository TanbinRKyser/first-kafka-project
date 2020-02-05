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
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
//        System.out.println("Hello World!!!");
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }
    private void run(){
        Logger logger = LoggerFactory.getLogger( ConsumerDemoWithThread.class );

        final String bootstrapServer = "127.0.0.1:9092";
        final String groupId = "my-sixth-app";
        final String topic = "first_topic";

        // latch dealing with multiple threads.
        CountDownLatch countDownLatch = new CountDownLatch( 1 );

        // Create a consumer runnable
        logger.info("Creating the consumer thread.");
        Runnable myConsumerRunnable = new ConsumerThread(
                countDownLatch, bootstrapServer, groupId, topic
        );

        // start the thread
        Thread myThread = new Thread( myConsumerRunnable );
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(
                () -> {
                    logger.info("Caught shutdown hook");
                    ( ( ConsumerThread ) myConsumerRunnable ).shutdown();
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        logger.info("Application has exited");
                    }
                }
        ) );
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error( "Application is interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger( ConsumerDemoWithThread.class );

        public ConsumerThread( CountDownLatch latch, String bootstrapServer, String groupId, String topic ){
            this.latch = latch;

            // Create a consumer config
            Properties properties = new Properties();
            properties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer );
            properties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
            properties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
            properties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, groupId );
            properties.setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );

            consumer = new KafkaConsumer< String, String >( properties );
            consumer.subscribe( Arrays.asList( topic ) );
        }

        @Override
        public void run() {
            // poll for new data
            try{
                while ( true ){
                    ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 100 ) );

                    for ( ConsumerRecord< String, String > record : records ){
                        logger.info( "Key: " + record.key() + ", Value: " + record.value() );
                        logger.info( "Partition: " + record.partition() + ", Offset: " + record.offset() );
                    }
                }
            } catch ( Exception ex ) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // Tell the main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //it is a special method to interrupt consumer.poll()
            consumer.wakeup();
        }
    }
}
