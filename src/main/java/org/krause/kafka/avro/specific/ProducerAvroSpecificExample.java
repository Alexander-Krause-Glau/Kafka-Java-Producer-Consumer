package org.krause.kafka.avro.specific;

import example.avro.Message;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerAvroSpecificExample {

  // Start docker-compose-avro.yml
  // You have to change the KAFKA_ADVERTISED_LISTENERS IP

  public static void main(String[] args) throws InterruptedException, IOException {

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "http://localhost:9092");
    properties.put("acks", "all");
    properties.put("retries", "1");
    properties.put("batch.size", "16384");
    properties.put("linger.ms", "1");
    properties.put("max.request.size", "2097152");
    properties.put("buffer.memory", 33554432);

    properties.put("key.serializer", KafkaAvroSerializer.class.getName());
    properties.put("value.serializer", KafkaAvroSerializer.class.getName());
    properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:9095");

    properties.put("kafka.topic", "example-topic");

    runMainLoop(args, properties);
  }

  private static void runMainLoop(String[] args, Properties properties)
      throws InterruptedException, UnsupportedEncodingException {

    try (KafkaProducer<String, Message> producer = new KafkaProducer<>(properties)) {

      while (true) {

        Thread.sleep(1000);
        final String id = "device-" + getRandomNumberInRange(1, 5);

        ProducerRecord<String, Message> record =
            new ProducerRecord<>(properties.getProperty("kafka.topic"), id, getMsg(id));

        final Future<RecordMetadata> r = producer.send(record);

        RecordMetadata recordMetadata = null;

        try {
          recordMetadata = r.get();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }

        System.out.printf("Send data with id: %s, metaData: %s at time = %d\n", id, recordMetadata,
            System.currentTimeMillis());
      }
    }
  }

  private static Message getMsg(String id) throws UnsupportedEncodingException {

    String timestamp = new Timestamp(System.currentTimeMillis()).toString();

    Message msg = new Message();
    msg.setId(id);
    msg.setTimestamp(timestamp);
    msg.setData("Test data: This is a simple String");

    return msg;
  }

  private static int getRandomNumberInRange(int min, int max) {

    Random r = new Random();
    return r.ints(min, (max + 1)).findFirst().getAsInt();

  }

}
