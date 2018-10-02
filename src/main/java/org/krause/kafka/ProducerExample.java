package org.krause.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerExample {

  // Credit: https://gist.github.com/kleysonr/d76df87479cc884818ebe870d297d7e5
  
  // Start the Kafka and Zookeeper docker container with the included docker-compose file.
  // Then, start run the ConsumerExample and ProducerExample.

  public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

    // https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("acks", "all");
    properties.put("retries", "1");
    properties.put("batch.size", "16384");
    properties.put("linger.ms", "1");
    properties.put("max.request.size", "2097152");
    properties.put("buffer.memory", 33554432);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    properties.put("kafka.topic", "example-topic");

    runMainLoop(args, properties);
  }

  private static void runMainLoop(String[] args, Properties properties)
      throws InterruptedException, UnsupportedEncodingException {

    // Create Kafka producer
    try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {

      while (true) {

        Thread.sleep(1000);
        String id = "device-" + getRandomNumberInRange(1, 5);

        System.out.println("Send data with id: " + id + " at: " + System.currentTimeMillis());

        producer.send(new ProducerRecord<String, String>(properties.getProperty("kafka.topic"), id,
            getMsg(id)));

      }

    }

  }

  private static String getMsg(String id) throws UnsupportedEncodingException {

    Gson gson = new Gson();

    String timestamp = new Timestamp(System.currentTimeMillis()).toString();

    JsonObject obj = new JsonObject();
    obj.addProperty("id", id);
    obj.addProperty("timestamp", timestamp);
    obj.addProperty("data",
        Base64.getEncoder().encodeToString("Test data: This is a simple String".getBytes("UTF-8")));
    String json = gson.toJson(obj);

    return json;

  }

  private static int getRandomNumberInRange(int min, int max) {

    Random r = new Random();
    return r.ints(min, (max + 1)).findFirst().getAsInt();

  }

}
