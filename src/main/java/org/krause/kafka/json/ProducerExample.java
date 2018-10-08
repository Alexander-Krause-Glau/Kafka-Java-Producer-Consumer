package org.krause.kafka.json;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Base64;
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

public class ProducerExample {

  private static AtomicLong counter = new AtomicLong(0);
  private static boolean showPerformance = false;

  // Start docker-compose.yml

  public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

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

    try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {

      while (true) {

        if (!showPerformance) {
          Thread.sleep(1000);
        }

        String id = "device-" + getRandomNumberInRange(1, 5);

        final Future<RecordMetadata> r =
            producer.send(new ProducerRecord<String, String>(properties.getProperty("kafka.topic"),
                id, getMsg(id)));

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
