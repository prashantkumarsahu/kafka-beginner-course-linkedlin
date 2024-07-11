package io.coduktor.demos.kafka.opensearch.OpenSearchConsumer.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

  KafkaProducer<String, String> kafkaProducer;
  String topic;

  private final static Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

  public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
    this.kafkaProducer = producer;
    this.topic = topic;
  }

  @Override
  public void onOpen() throws Exception {

  }

  @Override
  public void onClosed() throws Exception {
    kafkaProducer.close();
  }

  @Override
  public void onMessage(String s, MessageEvent messageEvent) throws Exception {
    logger.info("Received message: " + messageEvent.getData());

    logger.info("Sending message to Kafka topic: " + topic);

    ProducerRecord record = new ProducerRecord(topic, messageEvent.getData());
    kafkaProducer.send(record);
  }

  @Override
  public void onComment(String s) throws Exception {

  }

  @Override
  public void onError(Throwable throwable) {
    logger.error("Error occurred: " + throwable.getMessage());
  }
}
