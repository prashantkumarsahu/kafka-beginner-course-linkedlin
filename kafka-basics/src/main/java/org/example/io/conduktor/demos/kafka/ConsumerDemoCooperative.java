package org.example.io.conduktor.demos.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCooperative {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
  public static void main(String[] args) {

    logger.info("I am a Kafka Consumer !");
    String groupId = "my-java-application";
    String topic = "demo_topic";

    // create consumer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("java.security.auth.login.config", "/Users/prashant.sahu/kafka-3.7.0-src/config/kraft/kafka_client_jaas.conf");
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());
    properties.setProperty("group.id", groupId);
    properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
    properties.setProperty("auto.offset.reset", "earliest");
    // none = consumer group must exist, earliest = read from beginning, latest = read from new messages

    // create the consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

    // subscribe to topic
     consumer.subscribe(Arrays.asList("demo_topic"));

    // poll data
    while(true){
      logger.info("Polling for new data");
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      for(ConsumerRecord<String, String> record: records){
        logger.info(record.key() + " : " + record.value());
        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
      }
    }

    // flush and close producer
  }
}
