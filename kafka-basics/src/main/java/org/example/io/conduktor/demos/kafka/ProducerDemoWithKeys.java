package org.example.io.conduktor.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
  public static void main(String[] args) throws InterruptedException {

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
    String topic = "demo_topic";

    for(int i=0; i<2;i++){
      for(int j=0;j<10;j++){
        String key = "id_" + j;
        String value = "hello world " + j;
        // create if same keys goes to same partition everytime.
        ProducerRecord<String,String> record = new ProducerRecord<>("demo_topic",key,value);
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            // executes every time a record is successfully sent or an exception is thrown
            if(e==null){
              // the record was successfully sent
              logger.info("Recieved new metadata \n" +
                  "Key = " + key + " | Partition = " + recordMetadata.partition() + "\n");
            }else{
              logger.error("Error while producing event.");
            }
          }
        });
      }
    }


    // send data


    // flush and close producer
    producer.flush();
    producer.close();
  }
}
