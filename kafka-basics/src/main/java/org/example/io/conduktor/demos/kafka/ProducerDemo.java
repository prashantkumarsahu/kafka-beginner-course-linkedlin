package org.example.io.conduktor.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
  public static void main(String[] args) {

    logger.info("I am a Kafka Producer !");
    // create producer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());

    // needed for Conduktor Playground
//    properties.setProperty("security.protocol", "SASL_SSL");
//    properties.setProperty("sasl.mechanism", "PLAIN");

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // send data
    ProducerRecord<String,String> record = new ProducerRecord<>("demo_topic", "hello world");
    producer.send(record);

    // flush and close producer
    producer.flush();
    producer.close();
  }
}
