package org.krause.kafka;

import com.google.gson.Gson;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerExample {

  public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

    // https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
    
    // See ProducerExample class for further setup instructions

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");

    properties.put("group.id", "example-group");

    properties.put("kafka.topic", "example-topic");

    runMainLoop(args, properties);
  }

  private static void runMainLoop(String[] args, Properties properties)
      throws InterruptedException, UnsupportedEncodingException {

    // Create Kafka consumer
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {

      consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));

      System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("partition = %s, offset = %d, key = %s, value = %s, time = %d\n",
              record.partition(), record.offset(), record.key(),
              decodeMsg(record.value()).getData(), System.currentTimeMillis());
        }

      }
    }

  }

  private static Message decodeMsg(String json) throws UnsupportedEncodingException {

    Gson gson = new Gson();

    Message msg = gson.fromJson(json, Message.class);

    byte[] encodedData = Base64.getDecoder().decode(msg.getData());
    msg.setData(new String(encodedData, "UTF-8"));

    return msg;
  }

}
