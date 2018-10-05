package org.krause.kafka.avro.specific;

import example.avro.Message;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerAvroSpecificExample {

  public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaAvroDeserializer.class.getName());

    properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:9095");

    // specific record, not GenericRecord
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

    properties.put("kafka.topic", "example-topic");

    runMainLoop(args, properties);
  }

  private static void runMainLoop(String[] args, Properties properties)
      throws InterruptedException, UnsupportedEncodingException {

    try (Consumer<String, Message> consumer = new KafkaConsumer<>(properties)) {

      consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));

      System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));

      while (true) {
        ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, Message> record : records) {
          System.out.printf("partition = %s, offset = %d, key = %s, value = %s, time = %d\n",
              record.partition(), record.offset(), record.key(), record.value(),
              System.currentTimeMillis());
        }
      }
    }

  }

}
