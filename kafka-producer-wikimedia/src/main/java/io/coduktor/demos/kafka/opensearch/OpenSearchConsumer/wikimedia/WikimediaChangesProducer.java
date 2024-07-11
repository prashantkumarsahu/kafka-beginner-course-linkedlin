package io.coduktor.demos.kafka.opensearch.OpenSearchConsumer.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.*;
public class WikimediaChangesProducer {
  public static void main(String[] args) throws InterruptedException {
    String bootstrapServer = "127.0.0.1:9092";

    // producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32kb
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


    // create Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    String topic = "wikimedia.recentchange";

    String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // kafka-console-consumer --bootstrap-server localhost:9092 --topic wikimedia.recentchange

    EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
    EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

    EventSource eventSource = builder.build();

    // start producer in a separate thread
    eventSource.start();

    // produce for 10 min and then stop
    TimeUnit.MINUTES.sleep(10);

  }

}
