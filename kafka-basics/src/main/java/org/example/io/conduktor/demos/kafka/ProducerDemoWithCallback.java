package org.example.io.conduktor.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
  public static void main(String[] args) throws InterruptedException {

    logger.info("I am a Kafka Producer !");
    // create producer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
    properties.setProperty("batch.size", "400");
    // needed for Conduktor Playground
//    properties.setProperty("security.protocol", "SASL_SSL");
//    properties.setProperty("sasl.mechanism", "PLAIN");

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for(int j=0;j<10;j++){
      for(int i=0; i<30; i++){
        ProducerRecord<String,String> record = new ProducerRecord<>("demo_topic", "hello world");
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            // executes every time a record is successfully sent or an exception is thrown
            if(e==null){
              // the record was successfully sent
              logger.info("Recieved new metadata \n" +
                  "Topic = " + recordMetadata.topic() + "\n" +
                  "Partition = " + recordMetadata.partition() + "\n" +
                  "Offset = " + recordMetadata.offset() + "\n" +
                  "Timestamp = " + recordMetadata.timestamp() + "\n");
            }else{
              logger.error("Error while producing event.");
            }
          }
        });
      }

      Thread.sleep(500);
    }

    // send data


    // flush and close producer
    producer.flush();
    producer.close();
  }
}
