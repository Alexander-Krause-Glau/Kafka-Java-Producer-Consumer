package org.krause.kafka.json;

import com.google.gson.Gson;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.krause.kafka.Message;

public class ConsumerExample {

  private static AtomicLong counter = new AtomicLong(0);
  private static boolean showPerformance = false;

  public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

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

    // Create Kafka consumer
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {

      consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));

      System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          if (showPerformance) {
            decodeMsg(record.value());
            counter.incrementAndGet();
          } else {
            System.out.printf("partition = %s, offset = %d, key = %s, value = %s, time = %d\n",
                record.partition(), record.offset(), record.key(), decodeMsg(record.value()),
                System.currentTimeMillis());
          }
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
