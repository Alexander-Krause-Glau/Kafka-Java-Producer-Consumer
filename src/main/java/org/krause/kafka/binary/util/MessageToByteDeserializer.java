package org.krause.kafka.binary.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.krause.kafka.Message;

public class MessageToByteDeserializer implements Deserializer<Message> {

  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  private final ByteBufferDeserializer byteBufferDeserializer = new ByteBufferDeserializer();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.byteBufferDeserializer.configure(configs, isKey);
  }

  @Override
  public Message deserialize(String topic, byte[] data) {
    final ByteBuffer buffer = this.byteBufferDeserializer.deserialize(topic, data);
    Message msg = deserializeMessage(buffer);
    return msg;
  }

  @Override
  public void close() {
    this.byteBufferDeserializer.close();
  }

  private Message deserializeMessage(ByteBuffer buffer) {
    // Read string value of message
    final String id = getString(buffer);
    final String timestamp = getString(buffer);
    final String data = getString(buffer);

    Message msg = new Message();
    msg.setId(id);
    msg.setTimestamp(timestamp);
    msg.setData(data);
    return msg;
  }

  private String getString(ByteBuffer buffer) {
    final int stringLength = buffer.getInt();
    final byte[] stringBytes = new byte[stringLength];
    buffer.get(stringBytes);
    return new String(stringBytes, DEFAULT_CHARSET);
  }

}
