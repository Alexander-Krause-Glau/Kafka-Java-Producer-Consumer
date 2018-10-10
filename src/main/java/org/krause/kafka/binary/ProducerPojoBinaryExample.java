package org.krause.kafka.binary;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.krause.kafka.Message;
import org.krause.kafka.binary.util.MessageToByteSerializer;

public class ProducerPojoBinaryExample {

  private static AtomicLong counter = new AtomicLong(0);
  private static boolean showPerformance = false;

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

    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", MessageToByteSerializer.class.getName());

    properties.put("kafka.topic", "example-topic");

    if (args.length == 1) {
      showPerformance = args[0].equals("performance");
      System.out.println("Performance logging enabled...");
    }

    if (showPerformance) {
      final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
      scheduler.scheduleAtFixedRate(new Runnable() {

        boolean initialized = false;

        @Override
        public void run() {
          if (initialized) {
            System.out.println(counter.get() + " msg/sec");
          } else {
            initialized = true;
          }
          counter.set(0);
        }
      }, 1, 1, TimeUnit.SECONDS);
    }

    runMainLoop(args, properties);
  }

  private static void runMainLoop(String[] args, Properties properties)
      throws InterruptedException, UnsupportedEncodingException {

    try (KafkaProducer<String, Message> producer = new KafkaProducer<>(properties)) {

      while (true) {

        if (!showPerformance) {
          Thread.sleep(1000);
        }

        String id = "device-" + getRandomNumberInRange(1, 5);

        ProducerRecord<String, Message> record =
            new ProducerRecord<>(properties.getProperty("kafka.topic"), id, getMsg(id));

        final Future<RecordMetadata> r = producer.send(record);

        RecordMetadata recordMetadata = null;

        if (showPerformance) {
          counter.incrementAndGet();
        } else {
          try {
            recordMetadata = r.get();
          } catch (ExecutionException e) {
            e.printStackTrace();
          }

          System.out.printf("Send data with id: %s, metaData: %s at time = %d\n", id,
              recordMetadata, System.currentTimeMillis());
        }

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
